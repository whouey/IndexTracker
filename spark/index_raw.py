import sys, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, lit
from pyspark.sql.types import DecimalType
import urllib.request

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: index_raw.py <ds> 
        where ds in format year_month_day, e.g., 2021_09_19
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("index_raw") \
        .getOrCreate()

    date = datetime.datetime.strptime(ds, '%Y_%m_%d')
    url_template = 'https://www.twse.com.tw/exchangeReport/MI_5MINS_INDEX?response=csv&date={}'

    url = url_template.format(date.strftime('%Y%m%d'))

    res = urllib.request.urlopen(url)
    data = res.read().split(b'\r\n')[2:3243]

    rdd = spark.sparkContext.parallelize(data) \
            .map(lambda x: x.decode('utf-8'))
            
    df = spark.read.csv(rdd)

    df = df.select(col(df.columns[0]).alias('time'), col(df.columns[1]).alias('index')) \

    df = df.withColumn('time', regexp_extract(col('time'), r'.*(\d{2}:\d{2}:\d{2}).*', 1)) \
            .withColumn('datetime', concat_ws(' ', lit(ds), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyy_MM_dd HH:mm:ss'), lit('+08:00'))) \
            .drop('time')
    
    df = df.withColumn('index', regexp_replace(col('index'), ',', '').cast(DecimalType(scale=2)))

    df.coalesce(1).write.mode('overwrite').parquet(f's3a://indextracker/tw/index/trn/{ds}')

    spark.stop()