FROM georocket/integration-tests-parent-image:1.2.0

ADD conf/s3cfg /root/.s3cfg
ADD conf/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml

ADD src/tester.groovy /
ADD src/Utils.groovy /
RUN chmod +x /tester.groovy

CMD [ "/tester.groovy" ]
