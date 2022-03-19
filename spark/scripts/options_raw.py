import sys, datetime
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, array, explode
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: options_raw.py <ds> 
        where ds in format year_month_day, e.g., 2021_09_19
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("options_raw") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.addPyFile(f'{sys.path[0]}/util.py')
    from util import extract_zipped_content, get_expiration_code_map

    rdd = sc.binaryFiles(f's3a://indextracker/tw/raw/options/*{ds}*') \
        .flatMap(lambda x: extract_zipped_content(x[1])) \
        .flatMap(lambda x: x.split(b'\r\n')[2:]) \
        .map(lambda x: x.decode('utf-8').replace(' ', ''))

    schema = StructType([
        StructField('date', StringType()),
        StructField('contract', StringType()),
        StructField('strike_price', StringType()),
        StructField('expire', StringType()),
        StructField('option_type', StringType()),
        StructField('time', StringType()),
        StructField('price', DecimalType(scale=2)),
        StructField('volume', IntegerType()),
    ])

    df = spark.read.csv(rdd, schema=schema)

    df = df.withColumn('contract', trim(col('contract'))) \
            .where(col('contract').isin(['TXO']))
    
    df = df.withColumn('datetime', concat_ws(' ', col('date'), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyyMMdd HHmmss'), lit('+08:00'))) \
            .drop('date', 'time')

    date = datetime.datetime.strptime(ds, '%Y_%m_%d')
    mapping_dict = get_expiration_code_map(date)
    mapping_expr = create_map(list(chain(*zip([lit(k) for k in mapping_dict.keys()], [array([lit(item) for item in v]) for v in mapping_dict.values()]))))


    df = df.withColumn('expire_code', mapping_expr[col('expire')]) \
            .withColumn('expire_code', explode(col('expire_code')))

    df.coalesce(1).write.mode('overwrite').parquet(f's3a://indextracker/tw/options/trn/{ds}')

    spark.stop()