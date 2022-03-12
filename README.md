# IndexTracker

> This project is the capstone project of **Data Engineering Nanodegree** of **Udacity**

## Introduction
For making a living nowadays, investments had been a must-have skill.  
However, the enormous amount of information makes it a hard time to investigate.  

As a practice of data engineering, as well as a research in finance instruments, this project is aim to the most significant and well-known subject in stock market of **Taiwan**, **Taiwan Capitalization Weighted Stock Index** (**TAIEX** in short) and the derivatives of the index.  

This project collects open data from official organizations and reliable 3rd party services that provides relating data. And then run ETL jobs, generates aggregation data and loads it into database for inspection eventually. Moreover, the whole infrastructure is cloud_based, and triggered automatically by schedule.

There is only very little financial knowledge covered.

## Features
+ completely cloud-based, consist of most used components such as S3, EMR(Spark), MWAA(Airflow) and Redshift.
+ provide aggregation data of different kind of financial instruments, especially for plotting and analyzing.
+ dynamically kick-off for cost efficiency.

## Architecture

[//]: # (graph)

S3, Amazon Managed Workflow for Apache Airflow, Amazon Elastic MapReduce, Redshift

## Data Source

The data mainly gathered from website of official institutions, the detail explorations is under [`/DataExploring`](DataExploring/README.md) .

## Setup

## Roadmap


## ISSUE

    monthly contract data redundant when as weekly contract


## Developing environment


        where ds in format year_month_day, e.g., 2021_09_19

        
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)


## Notice

in extra should be proper json
    set redshift publicly accessible

## Usage

```sql
select * from index 
join index_history on index.id = index_history.index_id 
where index.name = 'TAIEX' and index_history.scale = 5 
order by index_history.datetime
```

```sql
select * from derivative d 
join derivative_detail dd on dd.derivative_id = d.id 
join derivative_detail_history ddh on ddh.derivative_detail_id = dd.id 
where d.name = 'TX' and dd.expire_code = 'W' and ddh.scale = 5 
order by ddh.datetime
```

## References

https://docs.python-requests.org/en/latest/  
https://boto3.amazonaws.com/v1/documentation/api/latest/index.html 

https://github.com/leriel/pyspark-easy-start
https://github.com/aws/aws-mwaa-local-runner

## License