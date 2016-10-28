#!/usr/bin/env groovy

import static Utils.*
import groovy.xml.XmlUtil

expectedContents = new File("/data/berlin_alexanderplatz_mini.xml").text
expectedNode = new XmlSlurper().parseText(expectedContents)

/**
 * Export the whole data store of GeoRocket to a file and check if the
 * contents are as expected
 * @param georocketHost the host where GeoRocket is running
 */
def testExport(String georocketHost) {
    // export file
    run("curl -sS -X GET http://${georocketHost}:63020/store/ "
        + "-o /data/exported_berlin.xml", new File("/"))

    def exportedContents = new File("/data/exported_berlin.xml").text
    if (exportedContents.length() < 100) {
        logWarn("Response: $exportedContents")
    }

    if (exportedContents.trim().equalsIgnoreCase("Not Found") ||
            exportedContents.trim().equalsIgnoreCase("404") ||
            exportedContents.trim().equalsIgnoreCase("503")) {
        logWarn("Got 0 chunks.")
        return false
    }

    // compare file with exported file
    def exportedNode = new XmlSlurper().parseText(exportedContents)

    // compare number of children
    if (exportedNode.children().size() < expectedNode.children().size()) {
        logWarn("Expected ${expectedNode.children().size()} chunks. Got "
            + "${exportedNode.children().size()}.")
        return false
    } else if (exportedNode.children().size() > expectedNode.children().size()) {
        logFail("Expected ${expectedNode.children().size()} chunks. Got "
            + "${exportedNode.children().size()}.")
        System.exit(1)
    }

    // compare children
    for (exportedChild in exportedNode.children()) {
        def expectedChild = expectedNode.children().find {
            it.Building.'@gml:id' == exportedChild.Building.'@gml:id' }
        String exportedStr = XmlUtil.serialize(exportedChild)
        String expectedStr = XmlUtil.serialize(expectedChild)
        if (exportedStr.trim() != expectedStr.trim()) {
            logFail("Exported chunk does not match expected one")
            System.exit(1)
        }
    }

    return true
}

/**
 * Search for a building using a bounding box. Check if GeoRocket really
 * exports only one building and if it is the right one.
 * @param georocketHost the host where GeoRocket is running
 */
def testExportByBoundingBox(String georocketHost) {
    // export file using bounding box that relates to the one of the building
    // we are looking for
    run("curl -sS -X GET http://${georocketHost}:63020/store/"
        + "?search=13.04709829608855,52.33016111449572,13.047127397967273,52.330179231017645 "
        + "-o /data/exported_berlin_by_bbox.xml", new File("/"))

    // read exported file
    def exportedContents = new File("/data/exported_berlin_by_bbox.xml").text
    if (exportedContents.length() < 100) {
        logFail("Response: $exportedContents")
        System.exit(1)
    }

    // parser exported file
    def exportedNode = new XmlSlurper().parseText(exportedContents)

    // check if we have only exported one chunk
    if (exportedNode.children().size() != 1) {
        logFail("Expected 1 chunks. Got ${exportedNode.children().size()}.")
        System.exit(1)
    }

    // check if we found the right building
    def child = exportedNode.children().getAt(0)
    def gmlId = child.Building.'@gml:id'
    if (gmlId != 'ID_147_D') {
        logFail("Expected gml:id ID_147_D. Got ${gmlId}.")
        System.exit(1)
    }
}

def runTest(String georocketHost) {
    // wait for GeoRocket
    waitHttp("http://${georocketHost}:63020")

    // import file
    run("curl -sS -X POST http://${georocketHost}:63020/store "
        + "--data @data/berlin_alexanderplatz_mini.xml "
        + "-H Content-Type:application/xml", new File("/"))

    boolean exportOk = false
    for (int i = 0; i < 20; ++i) {
        // wait until GeoRocket has indexed the file
        logWait("GeoRocket indexer")
        Thread.sleep(1000)

        if (testExport(georocketHost)) {
            exportOk = true
            break
        }
    }

    if (!exportOk) {
        logFail("Export failed")
        System.exit(1)
    }

    // run bounding box test - we don't have to do this in a loop
    // because GeoRocket should already be up and running and all
    // chunks should have been indexed.
    testExportByBoundingBox(georocketHost)
}

String mode = args.length > 0 ? args[0] : null
if (!mode) {
    mode = "standalone"
}

if (mode == "standalone") {
    logTest("GeoRocket standalone ...")
    runTest("georocket_standalone")
    logSuccess()
} else if (mode == "mongo") {
    logTest("GeoRocket with MongoDB back-end ...")
    runTest("georocket_mongo")
    def chunkCountInMongo = run('mongo mongo/georocket --quiet '
        + '--eval "db.fs.chunks.count()"', null, true).trim()
    if (chunkCountInMongo != String.valueOf(expectedNode.children().size())) {
        logFail("Expected ${expectedNode.children().size()} chunks in "
            + "MongoDB. Got ${chunkCountInMongo}.")
        System.exit(1)
    }
    logSuccess()
} else if (mode == "s3") {
    logTest("GeoRocket with S3 back-end ...")
    waitHttp("http://s3:8000", "GET", 403)
    run("s3cmd mb s3://georocket")
    runTest("georocket_s3")
    objects = run("s3cmd ls s3://georocket/", null, true).split('\n')
    if (objects.length != expectedNode.children().size()) {
        logFail("Expected ${expectedNode.children().size()} objects in "
            + "S3. Got ${objects.length}.")
        System.exit(1)
    }
    logSuccess()
} else if (mode == "hdfs") {
    logTest("GeoRocket with HDFS back-end ...")
    waitHttp("http://hdfs:50070", "GET")
    run("/usr/local/hadoop/bin/hdfs dfsadmin -safemode get", null, false, 20)
    run("/usr/local/hadoop/bin/hdfs dfsadmin -safemode wait")
    runTest("georocket_hdfs")
    hdfsfiles = run("/usr/local/hadoop/bin/hdfs dfs -ls /georocket/", null, true).split('\n')
    if (hdfsfiles.length - 1 != expectedNode.children().size()) {
        logFail("Expected ${expectedNode.children().size()} files in "
            + "HDFS. Got ${hdfsfiles.length - 1}.")
        System.exit(1)
    }
    logSuccess()
}
