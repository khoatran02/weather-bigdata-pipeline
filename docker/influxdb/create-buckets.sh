#!/bin/sh
set -eu
org=$DOCKER_INFLUXDB_INIT_ORG
token=$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN
# Analytics goes back to 1949: yearly shards instead of thousands of weekly ones.
# 2.9 rejects infinite retention with a 52w shard, hence 200 years.
influx bucket create --org "$org" --token "$token" --name weather_analytics --retention 73000d --shard-group-duration 52w
influx bucket create --org "$org" --token "$token" --name pipeline_metrics --retention 14d
