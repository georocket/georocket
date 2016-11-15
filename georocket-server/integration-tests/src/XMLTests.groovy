import static Utils.*
import groovy.xml.XmlUtil

/**
 * Tests if GeoRocket can handle XML files correctly
 * @author Michel Kraemer
 */
class XMLTests {
    private String georocketHost

    static EXPECTED_CONTENTS = new File("/data/berlin_alexanderplatz_mini.xml").text
    static EXPECTED_NODE = new XmlSlurper().parseText(EXPECTED_CONTENTS)

    /**
     * Creates the test class
     * @param georocketHost the host where GeoRocket is running
     */
    XMLTests(String georocketHost) {
        this.georocketHost = georocketHost
    }

    /**
    * Export the whole data store of GeoRocket to a file and check if the
    * contents are as expected
    */
    def testExport() {
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
        if (exportedNode.children().size() < EXPECTED_NODE.children().size()) {
            logWarn("Expected ${EXPECTED_NODE.children().size()} chunks. Got "
                + "${exportedNode.children().size()}.")
            return false
        } else if (exportedNode.children().size() > EXPECTED_NODE.children().size()) {
            logFail("Expected ${EXPECTED_NODE.children().size()} chunks. Got "
                + "${exportedNode.children().size()}.")
            System.exit(1)
        }

        // compare children
        for (exportedChild in exportedNode.children()) {
            def expectedChild = EXPECTED_NODE.children().find {
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
     * Import an XML file into the GeoRocket store, wait until it has been
     * indexed and then check if the store's contents are as expected
     */
    def testImport() {
        // import file
        run("curl -sS -X POST http://${georocketHost}:63020/store "
            + "--data @data/berlin_alexanderplatz_mini.xml "
            + "-H Content-Type:application/xml", new File("/"))

        boolean exportOk = false
        for (int i = 0; i < 20; ++i) {
            // wait until GeoRocket has indexed the file
            logWait("GeoRocket indexer")
            Thread.sleep(1000)

            if (testExport()) {
                exportOk = true
                break
            }
        }

        if (!exportOk) {
            logFail("Export failed")
            System.exit(1)
        }
    }

    /**
    * Search for a building using a bounding box. Check if GeoRocket really
    * exports only one building and if it is the right one.
    */
    def testExportByBoundingBox() {
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

    /**
    * Search for buildings in an area where there should be none. Check if
    * GeoRocket really returns no chunks.
    */
    def testExportByBoundingBoxNone() {
        // export file using bounding box that relates to the one of the building
        // we are looking for
        run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=12.0,51.0,12.1,51.1 "
            + "-o /data/exported_berlin_by_bbox_none.xml", new File("/"))

        // read exported file
        def exportedContents = new File("/data/exported_berlin_by_bbox_none.xml").text
        if (exportedContents != "Not Found") {
            logFail("Response: $exportedContents")
            System.exit(1)
        }
    }

    /**
    * Search for a building by gml:id. Check if GeoRocket really
    * exports only one building and if it is the right one.
    */
    def testExportByGmlId() {
        // export file using bounding box that relates to the one of the building
        // we are looking for
        run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=ID_146_D "
            + "-o /data/exported_berlin_by_gmlid.xml", new File("/"))

        // read exported file
        def exportedContents = new File("/data/exported_berlin_by_gmlid.xml").text
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
        if (gmlId != 'ID_146_D') {
            logFail("Expected gml:id ID_146_D. Got ${gmlId}.")
            System.exit(1)
        }
    }

    /**
    * Search for a building by a generic attribute. Check if GeoRocket
    * really exports only one building and if it is the right one.
    */
    def testExportByGenericAttribute() {
        // export file using an attributes
        run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=TestBuilding2 "
            + "-o /data/exported_berlin_by_generic_attribute.xml", new File("/"))

        // read exported file
        def exportedContents = new File("/data/exported_berlin_by_generic_attribute.xml").text
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
        if (gmlId != 'ID_146_D') {
            logFail("Expected gml:id ID_146_D. Got ${gmlId}.")
            System.exit(1)
        }
    }

    /**
    * Search for a dummy string. Check if GeoRocket really returns no chunks.
    */
    def testExportNone() {
        // export file using bounding box that relates to the one of the building
        // we are looking for
        run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=foobar "
            + "-o /data/exported_berlin_none.xml", new File("/"))

        // read exported file
        def exportedContents = new File("/data/exported_berlin_none.xml").text
        if (exportedContents != "Not Found") {
            logFail("Response: $exportedContents")
            System.exit(1)
        }
    }

    /**
     * Delete all chunks and check if the store is empty
     */
    def testDelete() {
        // delete all chunks
        run("curl -sS -X DELETE http://${georocketHost}:63020/store/")

        boolean deleteOk = false
        for (int i = 0; i < 5; ++i) {
            // wait until GeoRocket has deleted the chunks
            logWait("GeoRocket indexer")
            Thread.sleep(1000)

            // export whole data store
            run("curl -sS -X GET http://${georocketHost}:63020/store/ "
                + "-o /data/exported_berlin_deleted.xml", new File("/"))

            // read exported file
            def exportedContents = new File("/data/exported_berlin_deleted.xml").text
            if (exportedContents != "Not Found") {
                logWarn("Response: $exportedContents")
            } else {
                deleteOk = true
                break
            }
        }

        if (!deleteOk) {
            logFail("Deleting failed")
            System.exit(1)
        }
    }
}
