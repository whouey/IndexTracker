import sys, io, configparser, zipfile, datetime
from subprocess import check_output
from itertools import chain
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, array, concat, arrays_zip, array_repeat, explode
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

spark_conf = SparkConf()

SPARK_DRIVER_HOST = check_output(["hostname", "-i"]).decode(encoding="utf-8").strip()
spark_conf.setAll(
    [
        (
            "spark.master",
            "spark://spark:7077",
        ),  # <--- this host must be resolvable by the driver in this case pyspark (whatever it is located, same server or remote) in our case the IP of server
        ("spark.app.name", "myApp"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
        (
            "spark.driver.bindAddress",
            "0.0.0.0",
        ),  # <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
        (
            "spark.driver.host",
            SPARK_DRIVER_HOST,
        ),  # <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
    ]
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("""
        Usage: options_raw.py <ds> 
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]
    
    config = configparser.ConfigParser()
    config.read('app.cfg')

    accessKeyId = config['AWS']['AWS_ACCESS_KEY_ID']
    secretAccessKey = config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = SparkSession\
        .builder.config(conf=spark_conf)\
        .appName("options_raw")\
        .getOrCreate()
    
    sc = spark.sparkContext
    hadoopConf = sc._jsc.hadoopConfiguration()

    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    sc.addPyFile('util.py')
    from util import extract_zipped_content, get_expiration_code_map

    # https://stackoverflow.com/a/50829888
    rdd = sc.binaryFiles(f's3a://indextracker/tw/raw/options/*{ds}*') \
        .flatMap(lambda x: extract_zipped_content(x[1])) \
        .flatMap(lambda x: x.split(b'\r\n')[2:]) \
        .map(lambda x: x.decode('utf-8').replace(' ', ''))

    schema = StructType([
        StructField('date', StringType()),
        StructField('contract', StringType()),
        StructField('strike_price', StringType()),
        StructField('expire', StringType()),
        StructField('type', StringType()),
        StructField('time', StringType()),
        StructField('price', DecimalType(scale=2)),
        StructField('volume', IntegerType()),
    ])

    df = spark.read.csv(rdd, schema=schema)

    df = df.withColumn('contract', trim(col('contract'))) \
            .where(col('contract').isin(['TXO']))
    
    # print(df.count())

    df = df.withColumn('datetime', concat_ws(' ', col('date'), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyyMMdd HHmmss'), lit('+08:00'))) \
            .drop('date', 'time')

    date = datetime.datetime.strptime(ds, '%Y_%m_%d')
    mapping_dict = get_expiration_code_map(date)
    mapping_expr = create_map(list(chain(*zip([lit(k) for k in mapping_dict.keys()], [array([lit(item) for item in v]) for v in mapping_dict.values()]))))


    df = df.withColumn('expire_code', mapping_expr[col('expire')]) \
            .withColumn('expire_code', explode(col('expire_code')))

    # df.explain()

    # for every batch is very small in size, reduce the partitions to reduce the overhead 
    df.coalesce(1).write.parquet(f's3a://indextracker/tw/options/trn/{ds}')

    spark.stop()