#!/bin/bash
( cd ./pyspark-easy-start && docker-compose -f docker-compose.yml -f ../docker-compose.override.yml up -d )