FROM debian

RUN apt-get update && apt-get install -y curl groovy mongodb-clients

RUN grape install org.apache.commons commons-exec 1.3

ADD tester.groovy /
ADD Utils.groovy /
RUN chmod +x /tester.groovy
ADD data /data

CMD [ "/tester.groovy" ]
