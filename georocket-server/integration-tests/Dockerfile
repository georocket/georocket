FROM georocket/integration-tests-parent-image:2.1.0

ADD src/*.groovy /
RUN chmod +x /tester.groovy

ENTRYPOINT [ "/tester.groovy" ]
