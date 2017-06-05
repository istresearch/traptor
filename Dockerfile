FROM python:2
MAINTAINER Marti Martinez <marti.martinez@istresearch.com>

ARG BUILD_NUMBER=0
ENV BUILD_NUMBER $BUILD_NUMBER

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# Copy over code
COPY . /usr/src/app
RUN pip install .

# Start Traptor
CMD ["python2", "traptor/traptor.py"]
