FROM python:2-onbuild
MAINTAINER Robert Dempsey <robert.dempsey@istresearch.com>

# Install Python requirements
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy over code
COPY . /usr/src/app

# Copy the settings file
COPY dockerfiles/settings.py /usr/src/app/traptor/settings.py

# Set up supervisord
RUN pip install supervisor
COPY dockerfiles/supervisord.conf /etc/supervisor/supervisord.conf
COPY dockerfiles/traptor-supervisord.conf /etc/supervisor/conf.d/traptor-supervisord.conf
RUN mkdir -p /var/log/supervisor

# Run supervisor
CMD ["/usr/local/bin/supervisord"]