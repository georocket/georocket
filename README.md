GeoRocket [![Build Status](https://travis-ci.org/georocket/georocket.svg?branch=master)](https://travis-ci.org/georocket/georocket)
=========

*GeoRocket is a high-performance data store for geospatial files.* It is schema
agnostic and format preserving. This means it supports a wide range of
geospatial formats and schemas. Any file you store in GeoRocket can be can be
completely restored later. No information is lost.

GeoRocket is ready for the Cloud. It is event-driven and scalable. GeoRocket
offers APIs and an HTTP interface which allows it to be integrated in existing
applications.


Usage
-----

``docker run --name georocket -d -p 63020:63020 georocket/georocket``

* GeoRocket is accesible over the Port 63020 
* Also the default volume is ``/data/georocket/storage``

Use a host directory as a data volume
-------------------------------------

``docker run --name georocket -d -p 63020:63020 -v /myStorage:/data/georocket/storage georocket/georocket``

This mounts the host directory ``myStorage`` into the container storage. Replace *myStorage* with your desired location.

Use MongoDB container as data store
-----------------------------------

**Start MongoDB container**
``docker run --name some-mongo -d mongo``

**Adjust the config**
The default GeoRocket config([here](https://github.com/georocket/georocket/blob/master/georocket-server/conf/georocketd.json)) expects a mongo instance on *localhost*. Therefore you have to create a new config with the following values. (Don't mind the remaining settings which are used nevertheless) 

```json
{
  "georocket.storage.class": "io.georocket.storage.mongodb.MongoDBStore",
  "georocket.storage.mongodb.host": "some-mongo",
  "georocket.storage.mongodb.database": "georocket"
}
```

**Start GeoRocket**
Start the GeoRocket container and mount the new config.

``docker run --link some-mongo:mongo -d -p 63020:63020 -v $(pwd)/georocketd.json:/usr/local/georocket-server/conf/georocketd.json georocket/georocket``

**Note:** The name of the mongo container has to match the name of the *link* parameter.
**See also:** [MongoDB Container](https://hub.docker.com/_/mongo/) 


License
-------

GeoRocket is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
