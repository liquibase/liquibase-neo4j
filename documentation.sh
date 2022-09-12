#!/usr/bin/env bash

set -euxo pipefail

docker build \
  --file documentation.Dockerfile \
  --tag liquibase/neo4j-docs:latest \
  .

docker run \
  --pull never \
  --publish "${1:-8000}":8000 \
  --volume "$(pwd)":/usr/src/app \
   liquibase/neo4j-docs:latest