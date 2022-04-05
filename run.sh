#!/usr/bin/env bash

docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker rmi $(docker images -q)

# Building the app
docker-compose build

# Running the app
docker-compose run app