# Instructions on how to build the Docker image:
# - Build GeoRocket with `./gradlew installDist`
# - Copy this file and .dockerignore to `georocket-server/build/install`
# - Run `docker build -t georocket/georocket georocket-server/build/install`

FROM java:openjdk-8-jre-alpine
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

# install required packages
RUN apk update && \
    apk add bash sed && \
    rm -rf /var/cache/apk/*

# add GeoRocket user
RUN adduser -D -u 1000 -h /usr/local/georocket-server georocket

# add GeoRocket distribution
ADD georocket-server /usr/local/georocket-server

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
    sed -i '2iES_JAVA_OPTS="-Des.insecure.allow.root=true"' /usr/local/georocket-server/elasticsearch/*/bin/elasticsearch

USER georocket
VOLUME /data/georocket/storage
EXPOSE 63020
WORKDIR /usr/local/georocket-server/bin
ENTRYPOINT ["./georocketd"]
