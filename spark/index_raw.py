import sys, configparser, datetime
from subprocess import check_output
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace
from pyspark.sql.functions import concat_ws, to_timestamp, to_utc_timestamp, lit
from pyspark.sql.types import DecimalType
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
        Usage: index_raw.py <ds> 
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]
    
    config = configparser.ConfigParser()
    config.read('app.cfg')

    accessKeyId = config['AWS']['AWS_ACCESS_KEY_ID']
    secretAccessKey = config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = SparkSession\
        .builder.config(conf=spark_conf)\
        .appName("index_raw")\
        .getOrCreate()
    
    sc = spark.sparkContext
    hadoopConf = sc._jsc.hadoopConfiguration()

    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    date = datetime.datetime.strptime(ds, '%Y_%m_%d')
    url_template = 'https://www.twse.com.tw/exchangeReport/MI_5MINS_INDEX?response=csv&date={}'

    url = url_template.format(date.strftime('%Y%m%d'))

    res = urllib.request.urlopen(url)
    data = res.read().split(b'\r\n')[2:3243]

    rdd = sc.parallelize(data) \
            .map(lambda x: x.decode('utf-8'))
            
    df = spark.read.csv(rdd)

    df = df.select(col(df.columns[0]).alias('time'), col(df.columns[1]).alias('index')) \

    df = df.withColumn('time', regexp_extract(col('time'), r'.*(\d{2}:\d{2}:\d{2}).*', 1)) \
            .withColumn('datetime', concat_ws(' ', lit(ds), col('time'))) \
            .withColumn('datetime', to_utc_timestamp(to_timestamp(col('datetime'), 'yyyy_MM_dd HH:mm:ss'), lit('+08:00'))) \
            .drop('time')
    
    df = df.withColumn('index', regexp_replace(col('index'), ',', '').cast(DecimalType(scale=2)))

    # for every batch is very small in size, reduce the partitions to reduce the overhead 
    df.coalesce(1).write.parquet(f's3a://indextracker/tw/index/trn/{ds}')

    spark.stop()