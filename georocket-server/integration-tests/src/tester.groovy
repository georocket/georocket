#!/usr/bin/env groovy

import static Utils.*
import static XMLTests.EXPECTED_NODE
import static GeoJsonTests.EXPECTED_FEATURE_COLL

def runXMLTests(String georocketHost) {
    // wait for GeoRocket
    waitHttp("http://${georocketHost}:63020")

    def xml = new XMLTests(georocketHost)

    // import file (waits until GeoRocket has imported the file)
    xml.testImport()

    // run other tests - we don't have to do this in a loop
    // because GeoRocket should already be up and running and all
    // chunks should have been indexed.
    xml.testExportByBoundingBox()
    xml.testExportByBoundingBoxNone()
    xml.testExportByKeyValue()
    xml.testExportByKeyValueNone()
    xml.testExportByGmlId()
    xml.testExportByGenericAttribute()
    xml.testExportNone()
    xml.testTags()
    xml.testProperties()
    xml.testAttributes()
}

def finishXMLTests(String georocketHost) {
    def xml = new XMLTests(georocketHost)

    // delete all chunks
    xml.testDelete()
}

def runGeoJsonTests(String georocketHost) {
    // wait for GeoRocket
    waitHttp("http://${georocketHost}:63020")

    def json = new GeoJsonTests(georocketHost)

    // import file (waits until GeoRocket has imported the file)
    json.testImport()

    // run other tests - we don't have to do this in a loop
    // because GeoRocket should already be up and running and all
    // chunks should have been indexed.
    json.testExportByBoundingBox()
    json.testExportByBoundingBoxNone()
    json.testExportByKeyValue()
    json.testExportByKeyValueNone()
    json.testExportByProperty()
    json.testExportNone()
}

def finishGeoJsonTests(String georocketHost) {
    def json = new GeoJsonTests(georocketHost)

    // delete all chunks
    json.testDelete()
}

def assertMongoDBChunkCount(expected) {
    def chunkCountInMongo = run('mongo mongo/georocket --quiet '
        + '--eval "db.fs.chunks.count()"', null, true).trim()
    assertEquals(chunkCountInMongo, String.valueOf(expected),
        "Expected ${expected} chunks in MongoDB. Got ${chunkCountInMongo}.")
}

def assertS3ObjectCount(expected) {
    def objects = run("mc ls minio/georocket/", null, true)
    if (expected == 0) {
        assertEquals("", objects, "Expected no objects in S3. Got ${objects}.")
    } else {
        objects = objects.split('\n')
        assertEquals(expected, objects.length,
            "Expected ${expected} objects in S3. Got ${objects.length}.")
    }
}

String mode = args.length > 0 ? args[0] : null
if (!mode) {
    mode = "standalone"
}

if (mode == "standalone" || mode == "h2") {
    logTest("GeoRocket $mode ...")
    def host = "georocket_$mode"

    runXMLTests(host)
    finishXMLTests(host)

    runGeoJsonTests(host)
    finishGeoJsonTests(host)

    logSuccess()
} else if (mode == "mongo") {
    logTest("GeoRocket with MongoDB back-end ...")
    def host = "georocket_mongo"

    runXMLTests(host)
    assertMongoDBChunkCount(EXPECTED_NODE.children().size())
    finishXMLTests(host)
    assertMongoDBChunkCount(0)

    runGeoJsonTests(host)
    assertMongoDBChunkCount(EXPECTED_FEATURE_COLL.features.size())
    finishGeoJsonTests(host)
    assertMongoDBChunkCount(0)

    logSuccess()
} else if (mode == "s3") {
    logTest("GeoRocket with S3 back-end ...")
    waitHttp("http://s3:9000", "GET", 403)
    run("mc alias set minio http://s3:9000 minioadmin minioadmin")
    run("mc mb minio/georocket")
    def host = "georocket_s3"

    runXMLTests(host)
    assertS3ObjectCount(EXPECTED_NODE.children().size())
    finishXMLTests(host)
    assertS3ObjectCount(0)

    runGeoJsonTests(host)
    assertS3ObjectCount(EXPECTED_FEATURE_COLL.features.size())
    finishGeoJsonTests(host)
    assertS3ObjectCount(0)

    logSuccess()
}
