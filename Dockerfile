FROM python:3.7-buster
MAINTAINER Andrew Carter <andrew.carter@twosixtech.com>

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
CMD ["python", "-m", "traptor.traptor"]
