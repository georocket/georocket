FROM georocket/integration-tests-parent-image:1.1.0

ADD conf/s3cfg /root/.s3cfg

ADD tester.groovy /
ADD Utils.groovy /
RUN chmod +x /tester.groovy

CMD [ "/tester.groovy" ]
