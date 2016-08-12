#!/usr/bin/env groovy

import static Utils.*
import groovy.xml.XmlUtil

expectedContents = new File("/data/berlin_alexanderplatz_mini.xml").text
expectedNode = new XmlSlurper().parseText(expectedContents)

def testExport(String georocketHost) {
    // export file
    run("curl -sS -X GET http://${georocketHost}:63020/store/ "
        + "-o /data/exported_berlin.xml", new File("/"))

    def exportedContents = new File("/data/exported_berlin.xml").text
    if (exportedContents.length() < 100) {
        println "WARN Response: $exportedContents"
    }

    if (exportedContents.trim().equalsIgnoreCase("Not Found") ||
            exportedContents.trim().equalsIgnoreCase("404") ||
            exportedContents.trim().equalsIgnoreCase("503")) {
        println("WARN Got 0 chunks.")
        return false
    }

    // compare file with exported file
    def exportedNode = new XmlSlurper().parseText(exportedContents)

    // compare number of children
    if (exportedNode.children().size() < expectedNode.children().size()) {
        println("WARN Expected ${expectedNode.children().size()} chunks. Got "
            + "${exportedNode.children().size()}.")
        return false
    } else if (exportedNode.children().size() > expectedNode.children().size()) {
        println("FAIL Expected ${expectedNode.children().size()} chunks. Got "
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
            println("FAIL Exported chunk does not match expected one")
            System.exit(1)
        }
    }

    return true
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
        println("WAIT GeoRocket indexer")
        Thread.sleep(1000)

        if (testExport(georocketHost)) {
            exportOk = true
            break
        }
    }

    if (!exportOk) {
        println("FAIL Export failed")
        System.exit(1)
    }
}

println "TEST GeoRocket standalone ..."
runTest("georocket_standalone")
println "OK   Success."

println "TEST GeoRocket with MongoDB back-end ..."
runTest("georocket_mongo")
def chunkCountInMongo = run('mongo mongo/georocket --quiet '
    + '--eval "db.fs.chunks.count()"', null, true).trim()
if (chunkCountInMongo != String.valueOf(expectedNode.children().size())) {
    println("FAIL Expected ${expectedNode.children().size()} chunks in "
        + "MongoDB. Got ${chunkCountInMongo}.")
    System.exit(1)
}
println "OK   Success."

println "TEST GeoRocket with S3 back-end ..."
waitHttp("http://s3:8000", "GET", 403)
run("s3cmd mb s3://georocket")
runTest("georocket_s3")
objects = run("s3cmd ls s3://georocket/store/", null, true).split('\n')
if (objects.length != expectedNode.children().size()) {
    println("FAIL Expected ${expectedNode.children().size()} objects in "
        + "S3. Got ${objects.length}.")
    System.exit(1)
}
println "OK   Success."
