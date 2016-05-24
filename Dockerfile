FROM isuper/java-oracle:jdk_8
MAINTAINER Michel Kraemer <michel.kraemer@igd.fraunhofer.de>

# compile and install GeoRocket server
ADD . /tmp/georocket
RUN cd /tmp/georocket && \
    ./gradlew :georocket-server:installDist && \
    cp -r /tmp/georocket/georocket-server/build/install/georocket-server /usr/local/ && \
    sed -i -e 's/\("georocket\.storage\.file\.path"\).*/\1: "\/data\/georocket\/storage",/g' /usr/local/georocket-server/conf/georocketd.json && \
    rm -rf /tmp/georocket /root/.gradle

VOLUME /data/georocket/storage
EXPOSE 63020
WORKDIR /usr/local/georocket-server/bin
ENTRYPOINT ["./georocketd"]
