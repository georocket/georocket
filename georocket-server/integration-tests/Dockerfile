FROM georocket/integration-tests-parent-image:1.1.0

ADD conf/s3cfg /root/.s3cfg

ADD src/tester.groovy /
ADD src/Utils.groovy /
RUN chmod +x /tester.groovy

CMD [ "/tester.groovy" ]
