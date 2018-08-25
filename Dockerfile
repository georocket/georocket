# Instructions on how to build the Docker image:
# - Build GeoRocket with `./gradlew installDist`
# - Run `docker build -t georocket .`

FROM openjdk:8-jre-alpine
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

# add GeoRocket user
RUN adduser -D -u 1000 -h /usr/local/georocket-server georocket

# always add Elasticsearch first so we can keep it in the Docker cache
ADD georocket-server/build/install/georocket-server/elasticsearch /usr/local/georocket-server/elasticsearch

# install required packages
# (libc6-compat is required for snappy compression used by the mongodb driver)
RUN apk update && \
    apk add bash sed libc6-compat && \
    rm -rf /var/cache/apk/*

# add GeoRocket distribution (everything except Elasticsearch)
ADD georocket-server/build/install/georocket-server/bin  /usr/local/georocket-server/bin
ADD georocket-server/build/install/georocket-server/conf /usr/local/georocket-server/conf
ADD georocket-server/build/install/georocket-server/docs /usr/local/georocket-server/docs
ADD georocket-server/build/install/georocket-server/lib  /usr/local/georocket-server/lib

# create required directories
RUN mkdir -p /data/georocket/storage && \
    chown -R georocket:georocket /data/georocket/storage && \
    export ELASTICSEARCH_HOME=$(set -- /usr/local/georocket-server/elasticsearch/*/; echo $1) && \
    mkdir -p ${ELASTICSEARCH_HOME}plugins && \
    set -ex && for dirs in /usr/local/georocket-server/bin/.vertx ${ELASTICSEARCH_HOME}config ${ELASTICSEARCH_HOME}logs ${ELASTICSEARCH_HOME}plugins; do \
        mkdir -p $dirs; \
        chown -R georocket:georocket $dirs; \
    done

# create a temporary folder for Elasticsearch ourselves
# see https://github.com/elastic/elasticsearch/pull/27659
RUN mkdir -p /tmp/elasticsearch
RUN chown -R georocket:georocket /tmp/elasticsearch
ENV ES_TMPDIR=/tmp/elasticsearch

# remove x-pack-ml module
# RUN find /usr/local/georocket-server/elasticsearch/ -type d -name x-pack-ml -delete
RUN rm -rf /usr/local/georocket-server/elasticsearch/*/modules/x-pack/x-pack-ml

# configure GeoRocket
RUN sed -i -e 's/\$GEOROCKET_HOME\/storage/\/data\/georocket\/storage/g' /usr/local/georocket-server/conf/georocketd.yaml && \
    sed -i -e 's/host: 127\.0\.0\.1/host: 0.0.0.0/' /usr/local/georocket-server/conf/georocketd.yaml

USER georocket
VOLUME /data/georocket/storage
EXPOSE 63020
WORKDIR /usr/local/georocket-server/bin
ENTRYPOINT ["./georocketd"]
