# PySpark easy start

A project show how to easily start a local spark cluster by the help of docker.

## Instructions

Below was how the project be integrated.

1. Have docker installed.
2. Add pyspark-easy-start as a git submodule under `/spark/pyspark-easy-start`.
3. Modify `docker-compose.yml`: set `volumes:` to `..:/app`
4. Open command prompt, change working directory to `pyspark-easy-start`.
5. Run cluster up by `docker-compose up -d`
6. Run commands inside container `work-env` by `docker-compose exec work-env <command>`, that is, run scripts in spark using one of those: 
    + `docker-compose exec work-env python <filename>.py [args]`
    + `docker-compose exec work-env spark-submit <filename>.py [args]`

## Notice

+ In pyspark-easy-start, the settings on spark configuration are mostly not necessary, the only required are:
    + `hadoopConf.set('fs.s3a.access.key', <accessKeyId>)`
    + `hadoopConf.set('fs.s3a.secret.key', <secretAccessKey>)`  

    However, those two above is not required after published to EMR.

+ All the work done in spark is the same as in `/DataExploring`.
+ Install extra codec like `big5` is troublesome, better using pure `utf-8`.
+ Install any package by doing:
    + build your own container image extended from [`bitnami-docker-spark
`](https://github.com/bitnami/bitnami-docker-spark)
    + modify the `image` in `docker-compose.yml`.
    + start `docker-compose` over.
+ Window functions is more effective then loops.
+ Too many partitions make too much overhead, be careful when joining or repartitioning.
+ For every batch of data is very small in size, reduce the partitions to reduce the overhead when writing.
+ Make good use of methods like explain, sample and show of PySpark DataFrame, but make sure they are removed when publishing.

## Big shout out

### [Spark by Example](https://sparkbyexamples.com/)

A very detailed and useful website when developing PySpark.