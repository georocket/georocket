<h1 align="center">
	<br>
	<br>
	<img width="500" src="https://georocket.io/images/logo.svg" alt="GeoRocket">
	<br>
	<br>
	<br>
</h1>

> A high-performance data store for geospatial files

[![Apache License, Version 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0) [![Build Status](https://travis-ci.org/georocket/georocket.svg?branch=master)](https://travis-ci.org/georocket/georocket)

GeoRocket can store geospatial files such as *3D city models (CityGML)*, *GML*
and *GeoJSON* files. Any file saved in GeoRocket can be completely restored
later. No information is lost.

Powered by popular Open-Source framework [Elasticsearch](https://www.elastic.co/),
GeoRocket offers a wide range of *high-speed search features*. You can perform
spatial queries and search for attributes, layers and tags.

GeoRocket is *ready for the Cloud*. It is event-driven and designed for
*high performance* and *scalability*. GeoRocket offers APIs and an HTTP
interface which allows it to be integrated in existing applications.

## Official website

For more information about GeoRocket including comprehensive documentation,
visit the official website:

&#128640; https://georocket.io

## Building

GeoRocket requires Java 8 or higher. Run the following command to build
all subprojects:

    ./gradlew build

The script automatically downloads the correct Gradle version, so you won't
have to do anything else. If everything runs successfully you may create a
distribution:

    ./gradlew installDist

If the command finishes successfully you will find the *client distribution*
under `georocket-cli/build/install` and the *server distribution* under
`georocket-server/build/install`.

### Building the Docker image

You may build the Docker image for GeoRocket Server locally with the following
commands:

    ./gradlew installDist
    docker build -t georocket .

## Running GeoRocket

First, follow the instructions on building the GeoRocket distribution above.

Then start the GeoRocket server with the following command:

    georocket-server/build/install/georocket-server/bin/georocketd

Run the command-line application as follows:

    georocket-cli/build/install/georocket-cli/bin/georocket

If you don't provide any arguments the command-line application will print
usage instructions.

### Running GeoRocket Server inside a Docker container

The fastest way to run GeoRocket Server is to use the Docker image from
Docker Hub:

    docker run --name georocket -d -p 63020:63020 georocket/georocket

This launches GeoRocket Server in a Docker container and makes it available at
<http://localhost:63020>.

Read the [full instructions](https://hub.docker.com/r/georocket/georocket/) on
the Docker image to get more information.

License
-------

GeoRocket is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
