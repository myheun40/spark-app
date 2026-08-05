import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split

load_dotenv()

def main():
    # Druid 및 IoTDB 접속 정보
    DRUID_HOST = os.environ.get("DRUID_HOST") 
    DRUID_PORT = os.environ.get("DRUID_PORT") # 지정되어 있을 경우 가져오고, 아니면 기본값으로 세팅
    
    IOTDB_HOST = os.environ.get("IOTDB_HOST")
    IOTDB_PORT = os.environ.get("IOTDB_PORT")
    IOTDB_USER = os.environ.get("IOTDB_USER")
    IOTDB_PASSWORD = os.environ.get("IOTDB_PASSWORD")

    if not all([DRUID_HOST, DRUID_PORT, IOTDB_HOST, IOTDB_PORT, IOTDB_USER, IOTDB_PASSWORD]):
            raise ValueError("Not all required environment variables are set. Please check your .env file.")

    # 1. SparkSession 생성
    spark = SparkSession.builder \
        .appName("DruidToIoTDBMigration") \
        .config("spark.driver.userClassPathFirst", "true") \
        .config("spark.executor.userClassPathFirst", "true") \
        .getOrCreate()

    # 2. Druid Query 설정
    druid_jdbc_url = f"jdbc:avatica:remote:url=http://{DRUID_HOST}:{DRUID_PORT}/druid/v2/sql/avatica/"

    print(f"Reading data from Druid: {druid_jdbc_url} ...")

    all_table_query= '''
    (SELECT
        CAST(TABLE_NAME AS VARCHAR(255)) AS TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME like 'MIBR0308_%') tbl
    '''

    # 3. 테이블 목록 확인
    table_df = (
        spark.read.format("jdbc")
        .option("url", druid_jdbc_url)
        .option("dbtable", all_table_query)
        .option("driver", "org.apache.calcite.avatica.remote.Driver")
        .load()
    )

    table_df.show(truncate=False)

    print("=== Druid Tables ===")
    table_list = [row.TABLE_NAME for row in table_df.select("TABLE_NAME").toLocalIterator()]
    print(f"Available Druid Tables: {table_list}, Table Count: {len(table_list)}")

    for table in table_list:
        try:
            print(f"===[Start] Migration MIBR0308 Druid Table: {table} ===")
            
            druid_query = f'''
            (SELECT
                __time,
                CAST(OriTime AS VARCHAR(255)) AS OriTime,
                CAST(Val AS VARCHAR(255)) AS Val
            FROM "{table}") tbl
            '''

            df = (
                spark.read.format("jdbc")
                .option("url", druid_jdbc_url)
                .option("dbtable", druid_query)
                .option("driver", "org.apache.calcite.avatica.remote.Driver")
                .load()
            )

            table_parts = table.split("_", 1)
            group_id = table_parts[0]
            device_id = table_parts[1] if len(table_parts) > 1 else "default"
            full_device_path = f"root.{group_id}.{device_id}"

            # 4. IoTDB 적재용 DataFrame 가공
            # 1) __time을 Long(Unix timestamp in ms)으로 형변환하여 Time 컬럼 생성
            # 2) device 컬럼 추가
            output_df = df.select(
                (col("__time").cast("long") * 1000).alias("Time"), # 첫문자 대문자여야 함
                lit(full_device_path).alias("Device"),
                col("OriTime"),
                (col("Val").cast("double").alias("Val"))
            )

            print(f"Writing data for Device [{full_device_path}]...")
            output_df.printSchema()
            output_df.show(5, truncate=False)

            # 5. IoTDB 적재
            iotdb_url = f"iotdb://{IOTDB_HOST}:{IOTDB_PORT}"
            print(f"Writing data to IoTDB ({iotdb_url})...")

            # tree 모델 드라이버 
            output_df.write \
                .format("org.apache.iotdb.spark.db") \
                .option("url", iotdb_url) \
                .option("user", IOTDB_USER) \
                .option("password", IOTDB_PASSWORD) \
                .save()

            print(f"=== [Success] {table} Migration completed. ===")
        except Exception as e:
            print(f"=== [Error] {table} Migration failed.===")
            print(f"Error: {e}")
            continue

    print("All Migration completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()