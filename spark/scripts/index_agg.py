import sys, configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, explode, lit, udf
from pyspark.sql.functions import row_number, first, last, max, min, avg, stddev
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: index_agg.py <ds> 
        where ds in format year_month_day, e.g., 2021_09_19
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("index_agg") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.addPyFile(f'{sys.path[0]}/util.py')
    from util import roundup_to_minutes

    df = spark.read.parquet(f's3a://indextracker/tw/index/trn/{ds}')
    
    scales = [1, 5, 15, 30, 60]

    roundup_to_minutes_udf = udf(roundup_to_minutes, TimestampType())

    window = Window.partitionBy(['scale', 'scale_time']).orderBy('datetime')
    window_unbound = Window.partitionBy(['scale', 'scale_time']).orderBy('datetime') \
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df = df.withColumn('scale', array([lit(x) for x in scales])) \
            .withColumn('scale', explode(col('scale'))) \
            .withColumn('scale_time', roundup_to_minutes_udf(col('datetime'), col('scale'))) \
            .withColumn('row', row_number().over(window)) \
            .withColumn('price_open', first(col('index')).over(window_unbound)) \
            .withColumn('price_high', max(col('index')).over(window_unbound)) \
            .withColumn('price_low', min(col('index')).over(window_unbound)) \
            .withColumn('price_close', last(col('index')).over(window_unbound)) \
            .withColumn('price_mean', avg(col('index')).over(window_unbound)) \
            .withColumn('price_std', stddev(col('index')).over(window_unbound)) \
            .where(col('row') == 1) \
            .select(col('scale'), col('scale_time').alias('datetime'), \
                    col('price_open'), col('price_high'), col('price_low'), col('price_close'), col('price_mean'), col('price_std')) \
             
    df.coalesce(1).write.mode('overwrite').parquet(f's3a://indextracker/tw/index/agg/{ds}')

    spark.stop()