#!/usr/bin/env bash

docker stop mydb
docker rm mydb
docker rmi app -f

# Building the app
docker-compose build

# Running the app
docker-compose run app