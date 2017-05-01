# A Docker container capable of running all GRR components.
FROM ubuntu:xenial
MAINTAINER Greg Castle github@mailgreg.com

COPY scripts/install_dependencies.sh /
RUN /install_dependencies.sh -c -p

# Copy the GRR code over.
ADD . /usr/src/grr/

# Install GRR
RUN /usr/src/grr/scripts/install_grr_server_venv.sh -s -i /usr/share/grr-server -r /usr/src/grr

COPY scripts/docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]

# Port for the admin UI GUI
EXPOSE 8000

# Port for clients to talk to
EXPOSE 8080

# Server config, logs, sqlite db
VOLUME ["/etc/grr", "/var/log", "/var/grr-datastore"]

CMD ["grr"]
