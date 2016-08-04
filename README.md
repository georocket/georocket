GeoRocket [![Build Status](https://travis-ci.org/georocket/georocket.svg?branch=master)](https://travis-ci.org/georocket/georocket)
=========

*GeoRocket is a high-performance data store for geospatial files.* It is schema
agnostic and format preserving. This means it supports a wide range of
geospatial formats and schemas. Any file you store in GeoRocket can be can be
completely restored later. No information is lost.

GeoRocket is ready for the Cloud. It is event-driven and scalable. GeoRocket
offers APIs and an HTTP interface which allows it to be integrated in existing
applications.

How to use the Docker image to run GeoRocket
--------------------------------------------

### Start a GeoRocket instance

    docker run --name georocket -d -p 63020:63020 georocket/georocket

This launches GeoRocket in a Docker container and makes it available at
<http://localhost:63020>.

### Use a host directory as a data volume

    docker run --name georocket -d -p 63020:63020 -v /my/own/storage:/data/georocket/storage georocket/georocket

The default storage location inside the container is `/data/georocket/storage`.
This command mounts the host directory `/my/own/storage` into the container.
Replace `/my/own/storage` with the directory on your host where GeoRocket should
store its chunks and index.

### Use a MongoDB container as data store

#### 1. Start MongoDB container

    docker run --name some-mongo -d mongo

#### 2. Adjust configuration

The [default GeoRocket config file](georocket-server/conf/georocketd.json)
expects a MongoDB instance on `localhost`. You have to create a new file with
the following content. 

```json
{
  "georocket.storage.class": "io.georocket.storage.mongodb.MongoDBStore",
  "georocket.storage.mongodb.connectionString": "mongodb://some-mongo",
  "georocket.storage.mongodb.database": "georocket"
}
```

#### 3. Start GeoRocket

Start the GeoRocket container and mount the new config file.

    docker run --link some-mongo:mongo -d -p 63020:63020 -v /path/to/configfile/georocketd.json:/usr/local/georocket-server/conf/georocketd.json -v /my/own/storage:/data/georocket/storage georocket/georocket

Replace `/path/to/configfile` with the name of the directory on your host where
your new config file is located. Additionally, replace `/my/own/storage` with
the directory where GeoRocket should store its index.

**Note:** The name of the MongoDB container has to match the name of the *link*
parameter. See also: [MongoDB Container](https://hub.docker.com/_/mongo/) 

License
-------

GeoRocket is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
