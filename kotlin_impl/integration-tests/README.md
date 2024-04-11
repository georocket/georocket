# GeoRocket Integration Tests

This directory contains integration tests for GeoRocket. The tests run against a Docker image named `georocket/georocket`.

## Prepare

You should build the GeoRocket image locally before you run the tests. Otherwise, they will run against the latest GeoRocket image from Docker Hub.

From the root project directory, run the following commands:

    ./gradlew installDist
    docker build -t georocket/georocket .
    
    cd integration-tests
    npm i

## Run all tests

You can run all tests in parallel with the following command:

    npm run test

Be aware that this command runs several Docker containers (GeoRocket instances and various databases) at the same time. This might consume of lot of memory.

## Run a single test

Besides running all integration tests at the same time, you may also run a single test only. For example, if you want to test against MongoDB run the following command:

    npm run test-mongo

Have a look at the `scripts` section of the [package.json](./package.json) file to see all available tests.

## Show container output

If one of the tests fails and you want to view the container output, you can use the following command:

    DEBUG=testcontainers:containers npm run test

For an individual test:

    DEBUG=testcontainers:containers npm run test-mongo

## Timeouts

If you run the tests for the first time on your machine, they might time out because pulling the Docker images takes too long. In this case, just run the tests again or manually pre-pull the images.
