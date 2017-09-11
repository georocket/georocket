# Instructions on how to build the Docker image:
# - Build GeoRocket with `./gradlew installDist`
# - Copy this file and .dockerignore to `georocket-server/build/install`
# - Run `docker build -t georocket/georocket georocket-server/build/install`

FROM openjdk:8-jre-alpine
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

# add GeoRocket user
RUN adduser -D -u 1000 -h /usr/local/georocket-server georocket

# always add Elasticsearch first so we can keep it in the Docker cache
ADD georocket-server/elasticsearch /usr/local/georocket-server/elasticsearch

# install required packages
RUN apk update && \
    apk add bash sed && \
    rm -rf /var/cache/apk/*

# add GeoRocket distribution (everything except Elasticsearch)
ADD georocket-server/bin  /usr/local/georocket-server/bin
ADD georocket-server/conf /usr/local/georocket-server/conf
ADD georocket-server/docs /usr/local/georocket-server/docs
ADD georocket-server/lib  /usr/local/georocket-server/lib

# create required directories
RUN mkdir -p /data/georocket/storage && \
    chown -R georocket:georocket /data/georocket/storage && \
    export ELASTICSEARCH_HOME=$(set -- /usr/local/georocket-server/elasticsearch/*/; echo $1) && \
    mkdir -p ${ELASTICSEARCH_HOME}plugins && \
    set -ex && for dirs in /usr/local/georocket-server/bin/.vertx ${ELASTICSEARCH_HOME}config ${ELASTICSEARCH_HOME}logs ${ELASTICSEARCH_HOME}plugins; do \
        mkdir -p $dirs; \
        chown -R georocket:georocket $dirs; \
    done

# configure GeoRocket
RUN sed -i -e 's/\$GEOROCKET_HOME\/storage/\/data\/georocket\/storage/g' /usr/local/georocket-server/conf/georocketd.yaml && \
    sed -i -e 's/host: 127\.0\.0\.1/host: 0.0.0.0/' /usr/local/georocket-server/conf/georocketd.yaml

USER georocket
VOLUME /data/georocket/storage
EXPOSE 63020
WORKDIR /usr/local/georocket-server/bin
ENTRYPOINT ["./georocketd"]
