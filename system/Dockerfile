# riak node
FROM basho/riak-kv:ubuntu-2.2.3 AS riak
COPY files/riak.user.conf /etc/riak/user.conf
ENV USER_CONF /etc/riak/user.conf
COPY files/riak.entrypoint.sh /docker-entrypoint.sh
CMD /docker-entrypoint.sh

# control node
FROM debian:bullseye AS control
RUN \
    apt-get update && \
    apt-get -y install \
        openjdk-11-jdk-headless \
        libjna-java \
        leiningen
RUN \
    apt-get update && \
    apt-get -y install \
        docker.io \
        gnuplot \
        wget \
        nano \
        less
CMD tail -f /dev/null
