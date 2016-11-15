FROM georocket/integration-tests-parent-image:1.3.0

ADD conf/s3cfg /root/.s3cfg
ADD conf/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml

ADD src/*.groovy /
RUN chmod +x /tester.groovy

ENTRYPOINT [ "/tester.groovy" ]
