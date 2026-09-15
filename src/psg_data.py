
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import uuid


spark = SparkSession.builder.master('spark://spark-master:7077') \
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.4') \
    .config("spark.jars.ivy", "/tmp/spark-ivy") \
    .appName('PSG Load').getOrCreate()


jdbc_url = "jdbc:postgresql://postgres:5432/psg_data"
table_name = "source.customers"

user = password = 'spark'


def execute_query(query: str) -> int:
    conn = None
    stmt = None

    try:
        props = spark._sc._gateway.jvm.java.util.Properties()
        props.setProperty("user", user)
        props.setProperty("password", password)

        driver = spark._sc._gateway.jvm.org.postgresql.Driver()

        conn = driver.connect(jdbc_url, props)

        stmt = conn.createStatement()
        return stmt.executeUpdate(query)

    finally:
        if stmt is not None:
            stmt.close()

        if conn is not None:
            conn.close()


job_id = str(uuid.uuid4())

query = f''' INSERT INTO audit.etl_audit(id, job_name, task_name, status, start_time, 
    src_system, src_database, src_table, tgt_system, tgt_database, tgt_table, load_type, insert_dttm)
    VALUES ('{job_id}', 'TRUNCATE_LOAD', 'CUST_LOAD', 'P', current_timestamp, 'Postgres', 'Source',
    'customers', 'Postgres', 'Silver', 'customers', 'truncate_load', current_timestamp)'''

execute_query(query)


src_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", "spark") \
    .option("password", "spark") \
    .option("driver", "org.postgresql.Driver") \
    .load()


src_count = src_df.count()

query = f''' UPDATE audit.etl_audit SET src_count = {src_count}, update_dttm = current_timestamp WHERE "id" = '{job_id}' '''
execute_query(query)


deleted_count = execute_query("""DELETE FROM silver.customers WHERE 1=1""")

print(f'Deleted Row Count - {deleted_count}')

query = f''' UPDATE audit.etl_audit SET record_deleted = {deleted_count}, update_dttm = current_timestamp WHERE "id" = '{job_id}' '''
execute_query(query)


trns_df = src_df
for schema in src_df.schema:
    trns_df = trns_df.withColumnRenamed(schema.name, schema.name.replace(' ', '_').lower())

trns_df = trns_df.withColumn('insert_dttm', F.current_timestamp())


trns_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "silver.customers") \
    .option("user", user) \
    .option("password", password) \
    .mode("append") \
    .save()


inserted_count = trns_df.count()

print(f'inserted row count - {inserted_count}')

query = f''' UPDATE audit.etl_audit SET record_inserted = {inserted_count}, status = 'C', end_time = current_timestamp, 
    tgt_count = {inserted_count}, update_dttm = current_timestamp WHERE "id" = '{job_id}' '''
execute_query(query)


spark.stop()


