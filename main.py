import pymysql #レイヤー
import csv #CSVファイルを処理するモジュール
import io #CSVに変換するモジュール
import os
import boto3
from datetime import datetime

# Lambda関数の設定画面で入力した環境変数の値をPythonの変数に読み込む
# onモジュールのenvironオブジェクトは
# 現在実行中のプログラムがアクセスできる全ての環境変数をディクショナリで保持
# environオブジェクトのget()はキーが存在しなくてもNoneを返すので安全
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASS = os.environ.get('MYSQL_PASS')
MYSQL_DB = os.environ.get('MYSQL_DB')
S3_BUCKET = os.environ.get('S3_BUCKET')

s3_client = boto3.client('s3')
# boto3モジュールのclientメソッド
# AWSをPythonで操作するためのクライアントを作る※今回はS3

def lambda_handler(event, context): # ハンドラ関数
    
    connection = None # 変数を初期化
    
    try:
        # RDSとpymysqlライブラリのconnect関数でDBに接続
        connection = pymysql.connect( # 接続オブジェクトconnectionを定義
            host=MYSQL_HOST, # 環境変数をキーワード引数に
            user=MYSQL_USER, 
            password=MYSQL_PASS, 
            database=MYSQL_DB,
            port=3306 # MySQLのポート番号※世界共通
        )
        cursor = connection.cursor()
        # pymysqlライブラリにおいて接続オブジェクトを使って
        # 命令を実行し結果を読み込む窓口※カーソルメソッド
        
        # 実行前にDBに'inventory'テーブルが必要 まだ作ってない
        cursor.execute("SELECT id, product_name, quantity, created_at FROM ShoeFuwa20251023")
        # cursorオブジェクトのexecuteメソッドで引数のSQLを実行させる
        # 取り出す、カンマ区切りのデータを、表ShoeFuwa20251023から
        records = cursor.fetchall() #cursorオブジェクトの結果をすべて[()]で変数に代入
        
        if not records: # recordsが空なら
            print("No data found in inventory table. Export skipped.")
            return {'statusCode': 200, 'body': 'No data'}

        # CSVファイルのヘッダーに使う変数を定義
        header = ['id', 'product_name', 'quantity', 'created_at']
            
        # データをCSVに変換し、仮想ファイルに保持
        csv_buffer = io.StringIO() 
        # ioモジュールのStringIOクラスでcsv_bufferオブジェクトを生成
        csv_writer = csv.writer(csv_buffer)
        # csv_bufferオブジェクトの仮想ファイルにcsvモジュールのwriter関数で書き込む

        csv_writer.writerow(header) 
        # csv_writerオブジェクトのwriterowメソッドでheaderリストを仮想ファイルに書き込む
        csv_writer.writerows(records)
        csv_content = csv_buffer.getvalue()
        # 仮想ファイルオブジェクトのgetvalueメソッドで書き込まれた全ての文字列データを返す

        # S3へアップロード
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S') #現在時刻を取得
        TARGET_KEY = f"mysql-exports/ShoeFuwa20251023-export-{timestamp}.csv"
        # S3のパスを定義してファイルの場所と名前を特定
        # パスの内容S3バケット内のフォルダ名/RDSのテーブル名-exports-/現在時刻
        # S3_BUCKETのmysql-exportsフォルダにTARGET_KEYファイルがいる
        
        s3_client.put_object( 
        # boto3モジュールのS3クライアントオブジェクトのput_objectメソッド
        # 引数で指定されたファイルをS3へアップロードするメソッド
            Bucket=S3_BUCKET,
            Key=TARGET_KEY,
            Body=csv_content.encode('utf-8'),
            # 仮想ファイルに書き込まれたheaderやrecordsの値が
            # csv.contentに入っているから、それをオブジェクトとして
            # encodeメソッドを使いBodyへ書き出している
            ContentType='text/csv'
        )
        
        print(f"Successfully uploaded {len(records)} records to s3://{S3_BUCKET}/{TARGET_KEY}")
        #Tryで成功した場合にCloudWatch logsへ表示
        
        return {'statusCode': 200, 'body': 'Export successful'} #今のところ手動なのでlambdaのコンソールが呼び出し元

    except Exception as e:
        print(f"Execution Error: {e}") # 例外の場合CloudWatch Logsに出力
        raise e # lambdaの実行環境にエラーであると伝える※処理が中断される
        
    finally: # tryが成功してもexceptでも実行されるブロック
        if connection: # connectionオブジェクトが存在するか？
            connection.close() # データベースとの接続を閉じる
