# Instructions on how to build the Docker image:
# - Build GeoRocket with `./gradlew installDist`
# - Copy this file to `georocket-server/build/install`
# - Run `docker build -t georocket/georocket georocket-server/build/install`

FROM java:openjdk-8-jre-alpine
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

# add GeoRocket distribution
ADD georocket-server /usr/local/georocket-server

# install required packages and configure GeoRocket
RUN apk update && \
    apk add bash sed && \
    rm -rf /var/cache/apk/* && \
    sed -i -e 's/\$GEOROCKET_HOME\/storage/\/data\/georocket\/storage/g' /usr/local/georocket-server/conf/georocketd.yaml && \
    sed -i '2iES_JAVA_OPTS="-Des.insecure.allow.root=true"' /usr/local/georocket-server/elasticsearch/*/bin/elasticsearch

VOLUME /data/georocket/storage
EXPOSE 63020
WORKDIR /usr/local/georocket-server/bin
ENTRYPOINT ["./georocketd"]
