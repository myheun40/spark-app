import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split

def main():
    # Druid 및 IoTDB 접속 정보
    DRUID_HOST = os.environ.get("DRUID_HOST", "druid-druid-cluster-brokers.druid.svc.cluster.local")
    DRUID_PORT = os.environ.get("DRUID_PORT", "8082")
    DRUID_TABLE = os.environ.get("DRUID_TABLE", "MIBR0307_0XA_FT01A_XQ02")
    
    IOTDB_HOST = os.environ.get("IOTDB_HOST", "jdbc-balancer.iotdb-v2.svc.cluster.local")
    IOTDB_PORT = os.environ.get("IOTDB_PORT", "6667")
    IOTDB_USER = os.environ.get("IOTDB_USER", "root")
    IOTDB_PASSWORD = os.environ.get("IOTDB_PASSWORD", "root")

    table_parts = DRUID_TABLE.split("_", 1)
    group_id = table_parts[0]
    device_id = table_parts[1] if len(table_parts) > 1 else "default"
    full_device_path = f"root.{group_id}.{device_id}"

    # 1. SparkSession 생성
    spark = SparkSession.builder \
        .appName("DruidToIoTDBMigration") \
        .config("spark.driver.userClassPathFirst", "true") \
        .config("spark.executor.userClassPathFirst", "true") \
        .getOrCreate()

    # 2. Druid Query 설정
    druid_jdbc_url = f"jdbc:avatica:remote:url=http://{DRUID_HOST}:{DRUID_PORT}/druid/v2/sql/avatica/"

    print(f"Reading data from Druid: {druid_jdbc_url} ...")
    druid_query = f'''
    (SELECT
        __time,
        CAST(OriTime AS VARCHAR(255)) AS OriTime,
        CAST(Val AS VARCHAR(255)) AS Val
    FROM "{DRUID_TABLE}") tbl
    '''
    print(f"Druid Query: {druid_query}")


    # 3. Druid 데이터 로드
    df = (
        spark.read.format("jdbc")
        .option("url", druid_jdbc_url)
        .option("dbtable", druid_query)
        .option("driver", "org.apache.calcite.avatica.remote.Driver")
        .load()
        .selectExpr("__time", "OriTime", "Val")
    )

    # 4. IoTDB 적재용 DataFrame 가공
    # 1) __time을 Long(Unix timestamp in ms)으로 형변환하여 Time 컬럼 생성
    # 2) device 컬럼 추가
    output_df = df.select(
        (col("__time").cast("long") * 1000).alias("Time"),
        lit(full_device_path).alias("Device"),
        col("OriTime"),
        col("Val")
    )

    print("=== Final IoTDB Output Data Sample ===")
    output_df.printSchema()
    output_df.show(5, truncate=False)

    # 5. IoTDB 적재
    iotdb_url = f"iotdb://{IOTDB_HOST}:{IOTDB_PORT}"
    print(f"Writing data to IoTDB ({iotdb_url})...")

    output_df.write \
        .format("org.apache.iotdb.spark.db") \
        .option("url", iotdb_url) \
        .option("user", IOTDB_USER) \
        .option("password", IOTDB_PASSWORD) \
        .option("sql_dialects", "JDBC") \
        .mode("append") \
        .save()

    print("Migration completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()