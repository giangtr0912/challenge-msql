#!/usr/bin/env bash

echo "Do cleaning before start the app"
if docker ps -q --filter "name=mydb" | grep -q .; then docker stop mydb && docker rm -fv mydb; fi
if docker ps -q --filter "name=app" | grep -q .; then docker stop app && docker rm -fv app; fi

echo "Start building the app"
docker-compose build

echo "Running the app"
docker-compose run app

echo "Export database to file: output/tamara_staging.sql"
mkdir -p output
docker exec mydb mysqldump -u root --password=root tamara_staging > output/tamara_staging.sql

echo "Stopping the containers"
docker-compose down
