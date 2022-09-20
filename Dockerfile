# Base Image
FROM harbor.olympus.mtn/screamingbunny/docker:alpine-python3

ADD obo/ /opt/obo
ADD docker_dev_start.sh /opt/obo/dev_start.sh
ADD requirements.txt /tmp

WORKDIR /opt/obo

# Package Installation
# Packages - https://pkgs.alpinelinux.org/packages
RUN pip install -r /tmp/requirements.txt && \
#
# Cleanup
apk del --no-cache .build-deps && \
rm -rf /var/cache/apk/* *.tar.gz* /usr/src /root/.cache /root/.gnupg /tmp/*

EXPOSE 8880


# Run command when container launches
CMD ["./dev_start.sh"]
