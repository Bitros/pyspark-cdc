#!/bin/bash
# This script is used to set up a Docker network and run a PostgreSQL container for testing purposes.

# create pg and connect to devcontainer
docker network create testnetwork

docker run -d --name postgres -p 5432:5432 --network testnetwork \
  -e http_proxy="" -e HTTP_PROXY=""\
  -e https_proxy="" -e HTTPS_PROXY=""\
  -e no_proxy="" -e NO_PROXY="" \
  -e POSTGRES_PASSWORD=postgres \
  docker.io/postgres:16

docker network connect testnetwork vscode_pyspark_cdc_devcontainer

# connect pg to host if you want to connect to it from VSCode
docker network connect bridge postgres
