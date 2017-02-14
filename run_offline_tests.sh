#!/bin/bash

docker-compose run --rm traptor --entrypoint bash -c 'cd /code && python -m pytest -vv --cache-clear --cov=traptor --cov-report html'
docker-compose down