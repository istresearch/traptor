#!/bin/bash

docker-compose run --rm traptor --entrypoint bash -c 'cd /code && python -m pytest -v'
docker-compose down