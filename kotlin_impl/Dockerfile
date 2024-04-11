# Instructions on how to build the Docker image:
# - Build GeoRocket with `./gradlew installDist`
# - Run `docker build -t georocket .`

FROM ubuntu:21.10
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    # install required libraries for embedded MongoDB
    libcurl4 libssl1.1 \
  && rm -rf /var/lib/apt/lists/*

# add GeoRocket user
RUN adduser --disabled-password --gecos "" --uid 1000 --home /usr/local/georocket georocket

# add GeoRocket distribution
ADD build/install/georocket/bin  /usr/local/georocket/bin
ADD build/install/georocket/conf /usr/local/georocket/conf
ADD build/install/georocket/docs /usr/local/georocket/docs
ADD build/install/georocket/lib  /usr/local/georocket/lib

# create required directories
RUN set -ex && for dirs in /usr/local/georocket/bin/.vertx /data/georocket/storage; do \
        mkdir -p $dirs; \
        chown -R georocket:georocket $dirs; \
    done

# configure GeoRocket
RUN sed -i -e 's/\$GEOROCKET_HOME\/storage/\/data\/georocket\/storage/g' /usr/local/georocket/conf/georocket.yaml && \
    sed -i -e 's/host: 127\.0\.0\.1/host: 0.0.0.0/' /usr/local/georocket/conf/georocket.yaml

USER georocket
VOLUME /data/georocket/storage
EXPOSE 63020
WORKDIR /usr/local/georocket/bin
ENTRYPOINT ["./georocket"]
