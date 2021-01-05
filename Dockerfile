# Instructions on how to build the Docker image:
# - Build GeoRocket with `./gradlew installDist`
# - Run `docker build -t georocket .`

FROM openjdk:11-jre-slim
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

# add GeoRocket user
RUN adduser --disabled-password --gecos "" --uid 1000 --home /usr/local/georocket-server georocket

# add GeoRocket distribution
ADD georocket-server/build/install/georocket-server/bin  /usr/local/georocket-server/bin
ADD georocket-server/build/install/georocket-server/conf /usr/local/georocket-server/conf
ADD georocket-server/build/install/georocket-server/docs /usr/local/georocket-server/docs
ADD georocket-server/build/install/georocket-server/lib  /usr/local/georocket-server/lib

# create required directories
RUN set -ex && for dirs in /usr/local/georocket-server/bin/.vertx /data/georocket/storage; do \
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
