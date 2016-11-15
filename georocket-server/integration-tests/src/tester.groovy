#!/usr/bin/env groovy

import static Utils.*
import static XMLTests.EXPECTED_NODE

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
    xml.testExportByGmlId()
    xml.testExportByGenericAttribute()
    xml.testExportNone()
}

def finishXMLTests(String georocketHost) {
    def xml = new XMLTests(georocketHost)

    // delete all chunks
    xml.testDelete()
}

String mode = args.length > 0 ? args[0] : null
if (!mode) {
    mode = "standalone"
}

if (mode == "standalone") {
    logTest("GeoRocket standalone ...")
    runXMLTests("georocket_standalone")
    finishXMLTests("georocket_standalone")
    logSuccess()
} else if (mode == "mongo") {
    logTest("GeoRocket with MongoDB back-end ...")
    runXMLTests("georocket_mongo")
    def chunkCountInMongo = run('mongo mongo/georocket --quiet '
        + '--eval "db.fs.chunks.count()"', null, true).trim()
    assertEquals(chunkCountInMongo, String.valueOf(EXPECTED_NODE.children().size()),
        "Expected ${EXPECTED_NODE.children().size()} chunks in " +
        "MongoDB. Got ${chunkCountInMongo}.")
    finishXMLTests("georocket_mongo")
    logSuccess()
} else if (mode == "s3") {
    logTest("GeoRocket with S3 back-end ...")
    waitHttp("http://s3:8000", "GET", 403)
    run("s3cmd mb s3://georocket")
    runXMLTests("georocket_s3")
    objects = run("s3cmd ls s3://georocket/", null, true).split('\n')
    assertEquals(objects.length, EXPECTED_NODE.children().size(),
        "Expected ${EXPECTED_NODE.children().size()} objects in " +
        "S3. Got ${objects.length}.")
    finishXMLTests("georocket_s3")
    logSuccess()
} else if (mode == "hdfs") {
    logTest("GeoRocket with HDFS back-end ...")
    waitHttp("http://hdfs:50070", "GET")
    run("/usr/local/hadoop/bin/hdfs dfsadmin -safemode get", null, false, 20)
    run("/usr/local/hadoop/bin/hdfs dfsadmin -safemode wait")
    run("/usr/local/hadoop/bin/hdfs dfs -mkdir /georocket")
    run("/usr/local/hadoop/bin/hdfs dfs -chown georocket:georocket /georocket")
    runXMLTests("georocket_hdfs")
    hdfsfiles = run("/usr/local/hadoop/bin/hdfs dfs -ls /georocket/", null, true).split('\n')
    assertEquals(hdfsfiles.length - 1, EXPECTED_NODE.children().size(),
        "Expected ${EXPECTED_NODE.children().size()} files in " +
        "HDFS. Got ${hdfsfiles.length - 1}.")
    finishXMLTests("georocket_hdfs")
    logSuccess()
}
