FROM georocket/integration-tests-parent-image:3.0.0

ADD src/*.groovy /
RUN chmod +x /tester.groovy

ENTRYPOINT [ "/tester.groovy" ]
