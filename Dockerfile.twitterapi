FROM python:2
MAINTAINER Marti Martinez <marti.martinez@istresearch.com>

ARG BUILD_NUMBER=0
ENV BUILD_NUMBER $BUILD_NUMBER

ARG TWITTER_API_PORT=5000
ENV TWITTER_API_PORT $TWITTER_API_PORT

ARG TWITTER_API_WORKERS=8
ENV TWITTER_API_WORKERS $TWITTER_API_WORKERS

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy over code
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN pip install .

# Start Traptor Manager API
WORKDIR /usr/src/app/traptor/manager
CMD uwsgi --http :${TWITTER_API_PORT} -p ${TWITTER_API_WORKERS} -w wsgi
