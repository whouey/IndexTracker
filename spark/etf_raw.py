import sys, configparser, datetime
from subprocess import check_output
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, lit, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, BooleanType, ArrayType
import urllib.request

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
        Usage: etf_raw.py <ds> 
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]
    
    config = configparser.ConfigParser()
    config.read('app.cfg')

    accessKeyId = config['AWS']['AWS_ACCESS_KEY_ID']
    secretAccessKey = config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = SparkSession\
        .builder.config(conf=spark_conf)\
        .appName("etf_raw")\
        .getOrCreate()
    
    sc = spark.sparkContext
    hadoopConf = sc._jsc.hadoopConfiguration()

    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    sc.addPyFile('util.py')
    from util import get_paramed_url
    
    url_root = "https://api.finmindtrade.com/api/v4/data"
    date = datetime.datetime.strptime(ds, '%Y_%m_%d')

    params = {
        'dataset': 'TaiwanStockPriceTick',
        'data_id': '006204',
        'start_date': date.strftime('%Y-%m-%d'),
        'token': config['FinMind']['API_TOKEN'],
    }

    url = get_paramed_url(url_root, params)

    res = urllib.request.urlopen(url)
    data = res.read().decode('utf-8')

    rdd = sc.parallelize([data])
    
    df = spark.read.json(rdd)

    schema = ArrayType(StructType([
        StructField('date', StringType()),
        StructField('stock_id', StringType()),
        StructField('price', DecimalType()),
        StructField('time', StringType()),
        StructField('tick_type', BooleanType()),
    ]))

    df = df.withColumn('data', explode(col('data'))) \
            .select(
                col('data.date').alias('date'), 
                col('data.Time').alias('time'), 
                col('data.stock_id').alias('contract'),
                col('data.deal_price').alias('price'), 
                col('data.volume').alias('volume')
            )

    df = df.withColumn('datetime', concat_ws(' ', col('date'), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyy-MM-dd HH:mm:ss.SSS'), lit('+08:00'))) \
            .withColumn('datetime', date_trunc('second', col('datetime'))) \
            .drop('date', 'time')
    
    df = df.withColumn('price', col('price').cast(DecimalType(scale=2)))

    # for every batch is very small in size, reduce the partitions to reduce the overhead 
    df.coalesce(1).write.parquet(f's3a://indextracker/tw/etf/trn/{ds}')

    spark.stop()