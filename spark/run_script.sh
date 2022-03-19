#!/bin/bash
( cd ./pyspark-easy-start && docker-compose exec -T work-env python $1 $2 && sleep 10)