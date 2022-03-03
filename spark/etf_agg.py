import sys, configparser
from subprocess import check_output
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, explode, lit, udf
from pyspark.sql.functions import row_number, first, last, max, min, avg, stddev, sum
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

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
        Usage: etf_agg.py <ds> 
        """, file=sys.stderr)
        sys.exit(-1)

    ds = sys.argv[1]
    
    config = configparser.ConfigParser()
    config.read('app.cfg')

    accessKeyId = config['AWS']['AWS_ACCESS_KEY_ID']
    secretAccessKey = config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = SparkSession\
        .builder.config(conf=spark_conf)\
        .appName("etf_agg")\
        .getOrCreate()
    
    sc = spark.sparkContext
    hadoopConf = sc._jsc.hadoopConfiguration()

    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    
    sc.addPyFile('util.py')
    from util import roundup_to_minutes

    df = spark.read.parquet(f's3a://indextracker/tw/etf/trn/{ds}')
    
    scales = [1, 5, 15, 30, 60]

    roundup_to_minutes_udf = udf(roundup_to_minutes, TimestampType())

    window = Window.partitionBy(['scale', 'scale_time']).orderBy('datetime')
    window_unbound = Window.partitionBy(['scale', 'scale_time']).orderBy('datetime') \
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df = df.withColumn('scale', array([lit(x) for x in scales])) \
            .withColumn('scale', explode(col('scale'))) \
            .withColumn('scale_time', roundup_to_minutes_udf(col('datetime'), col('scale'))) \
            .withColumn('row', row_number().over(window)) \
            .withColumn('open', first(col('price')).over(window_unbound)) \
            .withColumn('high', max(col('price')).over(window_unbound)) \
            .withColumn('low', min(col('price')).over(window_unbound)) \
            .withColumn('close', last(col('price')).over(window_unbound)) \
            .withColumn('mean', avg(col('price')).over(window_unbound)) \
            .withColumn('std', stddev(col('price')).over(window_unbound)) \
            .withColumn('volume', sum(col('volume')).over(window_unbound)) \
            .where(col('row') == 1) \
            .select(col('scale'), col('scale_time').alias('datetime'), \
                    col('open'), col('high'), col('low'), col('close'), col('mean'), col('std'), col('volume')) \
             
    # for every batch is very small in size, reduce the partitions to reduce the overhead 
    df.coalesce(1).write.parquet(f's3a://indextracker/tw/etf/agg/{ds}')

    spark.stop()