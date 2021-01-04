FROM georocket/integration-tests-parent-image:2.0.0

ADD conf/s3cfg /root/.s3cfg

ADD src/*.groovy /
RUN chmod +x /tester.groovy

ENTRYPOINT [ "/tester.groovy" ]
