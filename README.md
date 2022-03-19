# IndexTracker

> This project is the capstone project of **Data Engineering Nanodegree** of **Udacity**

## Introduction
For making a living nowadays, investments had been a must-have skill.  
However, the enormous amount of information makes it a hard time to investigate.  

As a practice of data engineering, as well as a research in finance instruments, this project is aim to the most significant and well-known subject in stock market of **Taiwan**, **Taiwan Capitalization Weighted Stock Index** (**TAIEX** in short) and the derivatives of the index, and try to find the relationship between them.    

This project collects open data from official organizations and reliable 3rd party services that provides relating data. And then run ETL jobs, generates aggregation data and loads it into database for inspection eventually. Moreover, the whole infrastructure is cloud_based, and triggered automatically by schedule.

There is only very little financial knowledge covered.

## Features
+ completely cloud-based, consist of most used components such as S3, EMR(Spark), MWAA(Airflow) and Redshift.
+ provide aggregation data of different kind of financial instruments, for plotting and analyzing.

## Architecture  
![Components on AWS.](/assets/aws_architect.png)

Resource used:
+ Amazon Managed Workflows for Apache Airflow (MWAA) 
+ Amazon Simple Storage Service (S3) 
+ Amazon Elastic MapReduce (EMR) 
+ Amazon Redshift 

1. Whole workflow was triggered by MWAA and by schedule.
2. Download data from websites and APIs to S3 for backup.
3. Kick-off a EMR cluster and append jobs.
4. ETL by EMR, and put generated data back to S3.
5. Load staging data table to Redshift and transfer to final table.

## Data Sources

The data mainly gathered from website of official institutions.

+ [Taiwan Stock Exchange](https://www.twse.com.tw/en/)
+ [Taiwan Futures Exchange](https://www.taifex.com.tw/enl/eIndex)
+ [FinMind](https://finmindtrade.com/)

The detailed explorations is under [`/DataExploring`](DataExploring/README.md) .

## Data Model

![Data model.](/assets/data_model.png)

For the goal of this project is to organize the data of indices and different financial instruments together, and reveal the relationship between them.

The schema is in 3-NF for :
+ to clearly show a piece of historical data belongs which contract, derivative, and index, and the relationships of them. 
+ to easily query data of different indices and financial instruments in same scale together. 

The entities:
+ index - main subject of this project, currently contains only 1 record: **TAIEX**.
+ index_history - intraday data of an index.
+ derivative - derivative financial instrument of an index.
+ derivative_detail - a specific contract of a derivative.
+ derivative_detail_history - intraday data of an derivative contract.

The DDL script is under [`/sql`](sql/create_tables.sql) .

The data dictionary is provided [`here`](./DataDictionary.md)

## Setup

### Developing environment  

It's a bit complicated to building the developing environment.  

First, for airflow, the orchestrator, follow the instructions of [**aws-mwaa-local-runner**](https://github.com/aws/aws-mwaa-local-runner) to run mwaa inside docker, and check more information under [`/airflow`](airflow/README.md) .  

Second, to developing with spark without contaminate the environment, follow the
instructions of [**pyspark-easy-start**](https://github.com/leriel/pyspark-easy-start) to run the whole spark stack in docker. Still, check [`/spark`](spark/README.md) for detail.

Third, to work with Redshift, it's **NOT** recommended to developing on local environment or container, for Redshift is **DIFFERENT** from PostgreSQL after all. The most proper way is simply working with a Redshift cluster on AWS.  

### Production environment

Like the steps covered in [MWAA workshop](https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/), move the completed scripts onto S3, create the WMAA environment and Redshift cluster, make sure the connection of MWAA is set and the DDL script is ran of Redshift, it's good to go.

But notice that there are some more configurations must be done.

+ Redshift cluster should be set publicly accessible, which is very easily to be ignored.
+ For experimenting, public access mode of MWAA is much easier, for more information please check [this website](https://aws.amazon.com/premiumsupport/knowledge-center/mwaa-connection-timed-out-error/?nc1=h_ls) . 
+ To dynamically kick-off EMR, the execution role of MWAA should be granted permission of accessing EMR. If the permission given is AmazonEMRFullAccessPolicy_v2, then tag `for-use-with-amazon-emr-managed-policies=true` on EMR cluster is required to perform `RunJobFlow`. For more information please check [`this`](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-iam-policies.html).
+ When DAG of MWAA is performing **catchup**, the concurrency should be scale down, or increase the `poke_interval` of emr sensor to prevent the occurrence of ThrottlingException, please check [`this`](https://aws.amazon.com/tw/premiumsupport/knowledge-center/emr-cluster-status-throttling-error/) and [`this`](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-tuning.html).

## Roadmap

1. Find more subjects over the world.
2. Find the relationship between financial instruments and indices, by the means of statistics and machine learning.

## Issue

1. The sources used for the tasks for now is totally overkill, some lighter replacements such as AWS Glue, AWS Athena, AWS Lambda could be taken as consideration.
2. Monthly contract data redundant when weekly contract, however, considering the data usage, maybe NoSQL approaches is more suitable.
3. Code can be optimized to be more reuseable. 
4. To get the true benefit, need more resources and more accumulations.

## Usage

When querying history value of index by name, joined index and index_history together.

When querying history data of a derivative contract by name, must join derivative, derivative_detail, derivative_detail_history.

A regular candle stick diagram can be obtain by a query:
```sql
select * 
from index 
    join index_history on index.id = index_history.index_id 
where index.name = 'TAIEX' 
    and index_history.scale = 5 
    and datetime between '2022-03-03' and '2022-03-04'
order by index_history.datetime
```
And the diagram would be:
![sample](/assets/sample_candle.png)


To read more, [`check`](./validate.ipynb).

## Final words

The DAG of MWAA was triggered everyday on 18:00 local time, for all the data included will be released by then every trading day.

When the data amount goes up like 100 times, the resource used for now is all scalable and should works fine. 

While the user to serve increases, or has to fit more scenarios, however, to answer questions more specifically and efficiently, the data model should be transform to another like star-schema, furthermore, a NoSQL approach might be even better.

## References

First of all, to understand MWAA, check the references:
1. [Introducing Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/tw/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/)  
2. [Amazon MWAA for Analytics Workshop](https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/)  
3. [Amazon Managed Workflows for Apache Airflow User Guide](https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html)
4. [Running Spark Jobs on Amazon EMR with Apache Airflow: Using the new Amazon Managed Workflows for Apache Airflow (Amazon MWAA) Service on AWS](https://programmaticponderings.com/2020/12/24/running-spark-jobs-on-amazon-emr-with-apache-airflow-using-the-new-amazon-managed-workflows-for-apache-airflow-amazon-mwaa-service-on-aws/)
5. [aws-local-runner](https://github.com/aws/aws-mwaa-local-runner)
6. [pyspark-easy-start](https://github.com/leriel/pyspark-easy-start)
7. [Spark by Example](https://sparkbyexamples.com/)