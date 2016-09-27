FROM python:2-onbuild
MAINTAINER Robert Dempsey <robert.dempsey@istresearch.com>

# Install Python requirements
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy over code
COPY . /usr/src/app

# Start Traptor
ENTRYPOINT ["python2", "traptor/traptor.py"]