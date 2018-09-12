<a name="1.3.0"></a>
## [1.3.0-SNAPSHOT](https://github.com/georocket/georocket/compare/v1.2.0...master) (Upcoming)

### New features:

* Add low-latency optimistic merging
* Improve query performance
* Improve indexer performance
* Improve scalability during import/indexing
* Add cache for indexable chunks to reduce network load
* Add possibility to POST compressed files (GZIP)
* Add support for multiple Elasticsearch hosts (i.e. Elasticsearch cluster)
* Add possibility to automatically update the list of Elasticsearch hosts
* Return number of unmerged chunks in HTTP trailer
* Compress communication between GeoRocket and Elasticsearch (configurable)
* Enable snappy compression for MongoDB connection
* Support YAML syntax in environment variables
* Log memory info on startup

### Command-line application

* Display progress while importing
* Print metrics at the end of import process
* Compress communication with the GeoRocket server
* Add options to enable low-latency optimistic merging
* Print number of unmerged chunks

### Server API

* Improve usability of server API

### Client API

* Add possibility to enable low-latency optimistic merging
* Add API to get number of unmerged chunks

### Bug fixes

* Import a file only after it has been written/closed completely

### Internal

* Update Vert.x to 3.5.3
* Upgrade Elasticsearch to 6.3.2
* Upgrade Gradle Wrapper to 4.10
* Reduce log output

<a name="1.2.0"></a>
## [1.2.0](https://github.com/georocket/georocket/compare/v1.1.0...v1.2.0) (2018-07-13)

### New features:

* Add H2 back-end (now the default back-end!)
* Add support for SSL/TLS and HTTP/2
* Add support for Cross-Origin Resource Sharing (CORS)
* Allow HTTP compression to be configured
* Improve performance of XML splitter
* Allow users to search for `gmlId` specifically
* Add support for `gml:id` from GML 3.2
* Upgrade embedded Elasticsearch to version 5.6.5
* Increase heap size of embedded Elasticsearch
* Log progress while importing and deleting chunks
* Forward error messages from Elasticsearch to the HTTP interface
* Print banner and server version on startup
* Improve compatibility with Java 9

### Server API:

* Allow new HTTP endpoints to be added
* Allow extensions to register verticles
* Add possibility to filter service instances

### Bug fixes:

* Correctly import files with a UTF-8 BOM
* Correctly split GeoJSON files with UTF-8 characters
* Handle empty tags and properties correctly
* Make sure all results are returned when scrolling 
* Fix `StackOverflowError` that could happen when scrolling with a very large
  frame size

### Internal:

* Upgrade Vert.x to 3.5.1
* Update library dependencies
* Upgrade Gradle Wrapper to 4.8.1
* Improve integration tests and unit tests
* Use `rx.Completable` instead of `rx.Single<Void>`
* Make Observables cold
* Replace Rx operators by transformers
* Make it easier to build the Docker image

<a name="1.1.0"></a>
## [1.1.0](https://github.com/georocket/georocket/compare/v1.0.0...v1.1.0) (2017-09-11)

* Introduce 'properties'
* Add possibility to modify tags of existing chunks
* Add new comparison operators to query language
* Extend HTTP interface
* Add possibility to set default coordinate reference system (CRS) for queries
* Add possibility to specify coordinate reference system (CRS) in single query
* Improve support for UTF-8 encoded chunks
* Add possibility to override configuration values with environment variables
* Improve overall performance, usability, and stability

<a name="1.0.0"></a>
## [1.0.0](https://github.com/georocket/georocket/) (2017-01-26)

* Initial version
