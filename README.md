# GeoRocket [![Apache License, Version 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0) [![Build Status](https://travis-ci.org/georocket/georocket.svg?branch=master)](https://travis-ci.org/georocket/georocket) [![Dependency Status](https://www.versioneye.com/user/projects/57a79642c95322000fb8fdce/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57a79642c95322000fb8fdce)

*GeoRocket is a high-performance data store for geospatial files.* It is schema
agnostic and format preserving. This means it supports a wide range of
geospatial formats and schemas. Any file you store in GeoRocket can be can be
completely restored later. No information is lost.

GeoRocket is ready for the Cloud. It is event-driven and scalable. GeoRocket
offers APIs and an HTTP interface which allows it to be integrated in existing
applications.

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
    cp docker/Dockerfile georocket-server/build/install
    docker build -t georocket georocket-server/build/install

## Running GeoRocket

First follow the instructions on building the GeoRocket distribution above.

Then start the GeoRocket server with the following command:

    georocket-server/build/install/georocket/bin/georocketd

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
