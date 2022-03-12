import sys, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, lit, date_trunc
from pyspark.sql.types import DecimalType, IntegerType
import urllib.request

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: etf_raw.py <ds> 
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("etf_raw") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.addPyFile(f'{sys.path[0]}/util.py')
    from util import get_paramed_url
    
    url_root = "https://api.finmindtrade.com/api/v4/data"
    date = datetime.datetime.strptime(ds, '%Y_%m_%d')

    params = {
        'dataset': 'TaiwanStockPriceTick',
        'data_id': '006204',
        'start_date': date.strftime('%Y-%m-%d'),
    }

    url = get_paramed_url(url_root, params)

    res = urllib.request.urlopen(url)
    data = res.read().decode('utf-8')

    rdd = sc.parallelize([data])
    
    df = spark.read.json(rdd)

    df = df.withColumn('data', explode(col('data'))) \
            .select(
                col('data.date').alias('date'), 
                col('data.Time').alias('time'), 
                col('data.stock_id').alias('contract'),
                col('data.deal_price').alias('price').cast(DecimalType(scale=2)), 
                col('data.volume').alias('volume').cast(IntegerType())
            )

    df = df.withColumn('datetime', concat_ws(' ', col('date'), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyy-MM-dd HH:mm:ss.SSS'), lit('+08:00'))) \
            .withColumn('datetime', date_trunc('second', col('datetime'))) \
            .drop('date', 'time')

    # for every batch is very small in size, reduce the partitions to reduce the overhead 
    df.coalesce(1).write.mode('overwrite').parquet(f's3a://indextracker/tw/etf/trn/{ds}')

    spark.stop()