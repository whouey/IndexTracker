import sys, datetime
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, array, concat, arrays_zip, array_repeat, explode
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: futures_raw.py <ds> 
        where ds in format year_month_day, e.g., 2021_09_19
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("futures_raw") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.addPyFile(f'{sys.path[0]}/util.py')
    from util import extract_zipped_content, get_expiration_code_map

    rdd = sc.binaryFiles(f's3a://indextracker/tw/raw/futures/*{ds}*') \
        .flatMap(lambda x: extract_zipped_content(x[1])) \
        .flatMap(lambda x: x.split(b'\r\n')[1:]) \
        .map(lambda x: x.decode('utf-8'))

    schema = StructType([
        StructField('date', StringType()),
        StructField('contract', StringType()),
        StructField('expire', StringType()),
        StructField('time', StringType()),
        StructField('price', DecimalType(scale=2)),
        StructField('volume', IntegerType()),
        StructField('near_price', DecimalType(scale=2)),
        StructField('far_price', DecimalType(scale=2)),
    ])

    df = spark.read.csv(rdd, schema=schema)
    
    df = df.withColumn('contract', trim(col('contract'))) \
            .where(col('contract').isin(['TX', 'MTX']))

    df = df.withColumn('expire', trim(col('expire'))) \
            .withColumn('switch_expire', concat(array(col('expire')), split(col('expire'), '/'))) \
            .withColumn('switch_price', array(col('price'), col('near_price'), col('far_price'))) \
            .withColumn('switch_volume', concat(array(col('volume')), array_repeat((col('volume') / 2).cast('int'), 2))) \
            .withColumn('switch', arrays_zip(col('switch_expire'), col('switch_price'), col('switch_volume'))) \
            .withColumn('switch', explode(col('switch'))) \
            .withColumn('expire', col('switch.switch_expire')) \
            .withColumn('price', col('switch.switch_price')) \
            .withColumn('volume', col('switch.switch_volume')) \
            .where(col('price').isNotNull() & ~col('expire').contains('/')) \
            .drop('near_price', 'far_price', 'switch_expire', 'switch_price', 'switch_volume', 'switch')
    
    df = df.withColumn('datetime', concat_ws(' ', col('date'), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyyMMdd HHmmss'), lit('+08:00'))) \
            .drop('date', 'time')

    date = datetime.datetime.strptime(ds, '%Y_%m_%d')
    mapping_dict = get_expiration_code_map(date)
    mapping_expr = create_map(list(chain(*zip([lit(k) for k in mapping_dict.keys()], [array([lit(item) for item in v]) for v in mapping_dict.values()]))))

    df = df.withColumn('expire_code', mapping_expr[col('expire')]) \
            .withColumn('expire_code', explode(col('expire_code')))

    df.coalesce(1).write.mode('overwrite').parquet(f's3a://indextracker/tw/futures/trn/{ds}')

    spark.stop()