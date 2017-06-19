import groovy.json.JsonException
import groovy.json.JsonSlurper

import static Utils.*
import groovy.xml.XmlUtil

/**
 * Tests if GeoRocket can handle XML files correctly
 * @author Michel Kraemer
 */
class XMLTests extends StoreTests {
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

    @Override
    def testExport() {
        // export file
        def exportedContents = run("curl -sS -X GET " +
            "http://${georocketHost}:63020/store/", null, true)
        if (exportedContents.length() < 100) {
            logWarn("Response: $exportedContents")
        }

        // try to parse error response
        try {
            def result = new JsonSlurper().parseText(exportedContents)
            if (result.error && result.error.reason.trim().equals("Not Found")) {
                logWarn("Got 0 chunks.")
                return false
            }
        } catch (JsonException e) {
            // Ignore exception if the error could not be parsed. Either this
            // was no error or we will fail later.
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
            fail("Expected ${EXPECTED_NODE.children().size()} chunks. Got "
                + "${exportedNode.children().size()}.")
        }

        // compare children
        for (exportedChild in exportedNode.children()) {
            def expectedChild = EXPECTED_NODE.children().find {
                it.Building.'@gml:id' == exportedChild.Building.'@gml:id' }
            String exportedStr = XmlUtil.serialize(exportedChild)
            String expectedStr = XmlUtil.serialize(expectedChild)
            assertEquals(exportedStr.trim(), expectedStr.trim(),
                "Exported chunk does not match expected one")
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
            + "--data @data/berlin_alexanderplatz_mini.xml")
        testExportUntilOK()
    }

    /**
     * Assert that the given string contains exactly one Building with the given ID
     * @param exportedContents the string to parse
     * @param expectedId the expected building ID
     */
    private void assertExportedBuilding(exportedContents, expectedId) {
        String error = checkExportedBuilding(exportedContents, expectedId)
        if (error) {
            fail(error)
        }
    }

    /**
     * Check if the given string contains exactly one Building with the given ID
     * @param exportedContents the string to parse
     * @param expectedId the expected building ID
     */
    private String checkExportedBuilding(exportedContents, expectedId) {
        if (exportedContents.length() < 100) {
            return "Response: $exportedContents"
        }

        // parse exported file
        def exportedNode = new XmlSlurper().parseText(exportedContents)

        // check if we have only exported one chunk
        if (exportedNode.children().size() != 1) {
            return "Expected 1 chunks. Got ${exportedNode.children().size()}."
        }

        // check if we found the right building
        def child = exportedNode.children().getAt(0)
        def gmlId = child.Building.'@gml:id'
        if (!Objects.equals(gmlId, expectedId)) {
            return "Expected gml:id ${expectedId}. Got ${gmlId}."
        }
    }

    /**
     * Search for a building using a bounding box. Check if GeoRocket really
     * exports only one building and if it is the right one.
     */
    def testExportByBoundingBox() {
        // export file using bounding box that relates to the one of the building
        // we are looking for
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=13.04709829608855,52.33016111449572,13.047127397967273,52.330179231017645",
            null, true)
        assertExportedBuilding(exportedContents, 'ID_147_D')
    }

    /**
     * Search for a building by key-value pair. Check if GeoRocket really
     * exports only one building and if it is the right one.
     */
    def testExportByKeyValue() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=" + URLEncoder.encode('EQ(TestBuildingName TestBuilding2)', 'UTF-8'),
            null, true)
        assertExportedBuilding(exportedContents, 'ID_146_D')
    }

    /**
     * Search for a building by gml:id. Check if GeoRocket really
     * exports only one building and if it is the right one.
     */
    def testExportByGmlId() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=ID_146_D", null, true)
        assertExportedBuilding(exportedContents, 'ID_146_D')
    }

    /**
     * Search for a building by a generic attribute. Check if GeoRocket
     * really exports only one building and if it is the right one.
     */
    def testExportByGenericAttribute() {
        // export file using a generic attribute
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=TestBuilding2", null, true)
        assertExportedBuilding(exportedContents, 'ID_146_D')
    }

    /**
     * Add and delete tags
     */
    def testTags() {
        // ensure that no chunk with the test tags is found
        assertNotFound("test1")
        assertNotFound("test2")
        assertNotFound("test3")

        // append single test tag to the chunk
        run("curl -sS -X PUT http://${georocketHost}:63020/store/"
                + "?search=ID_146_D&tags=test1", null, true)

        // check if chunk is found by the added tag
        assertFound("test1", "ID_146_D")

        // append multiple test tags to the chunk
        run("curl -sS -X PUT http://${georocketHost}:63020/store/"
                + "?search=ID_146_D&tags=test2,test3", null, true)

        // check if the new tags are appended and the previous tag is still present
        assertFound("test1", "ID_146_D")
        assertFound("test2", "ID_146_D")
        assertFound("test3", "ID_146_D")

        // remove first tag
        run("curl -sS -X DELETE http://${georocketHost}:63020/store/"
                + "?search=ID_146_D&tags=test1", null, true)

        // check if all tags but the first are present
        assertNotFound("test1")
        assertFound("test2", "ID_146_D")
        assertFound("test3", "ID_146_D")

        // remove multiple tags
        run("curl -sS -X DELETE http://${georocketHost}:63020/store/"
                + "?search=ID_146_D&tags=test2,test3", null, true)

        // check if the tags are removed
        assertNotFound("test1")
        assertNotFound("test2")
        assertNotFound("test3")
    }

    /**
     * Set and delete properties
     */
    def testProperties() {
        // set test property
        run("curl -sS -X PUT http://${georocketHost}:63020/store/"
                + "?search=ID_146_D&properties="
                + URLEncoder.encode('test1:1', 'UTF-8'), null, true)

        // check if chunk is found by the set property
        assertFound("EQ(test1 1)", "ID_146_D")
        assertNotFound("EQ(test1 2)")
        
        // update property value
        run("curl -sS -X PUT http://${georocketHost}:63020/store/"
                + "?search=ID_146_D&properties="
                + URLEncoder.encode('test1:2', 'UTF-8'), null, true)

        // check if chunk is found by the updated property
        assertFound("EQ(test1 2)", "ID_146_D")
        assertNotFound("EQ(test1 1)")
        
        assertValues("", "property", "test1", ["2"])

        // update property value
        run("curl -sS -X PUT http://${georocketHost}:63020/store/"
                + "?search=ID_147_D&properties="
                + URLEncoder.encode('test1:3,test2:3', 'UTF-8'), null, true)

        assertValues("", "property", "test1", ["2", "3"])
        assertValues("", "property", "test2", ["3"])
    }

    /**
     * Assert that no chunk is found for the specified query
     * @param query the query to search for
     */
    def assertValues(def query, def field, def name, def values) {
        testUntilOK({
            def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
                    + "?search=" + URLEncoder.encode(query, 'UTF-8')
                    + "&$field=$name", null, true)
            try {
                List results = new JsonSlurper().parseText(exportedContents)
                Collections.sort(values)
                Collections.sort(results)
                Objects.equals(values, results)
            } catch (ignored) {
                return false
            }
        }, "GeoRocket metadata", "Values for $name not found")
    }

    /**
     * Assert that no chunk is found for the specified query
     * @param query the query to search for
     */
    def assertNotFound(def query) {
        testUntilOK({
            def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
                    + "?search=" + URLEncoder.encode(query, 'UTF-8'), null, true)
            try {
                def result = new JsonSlurper().parseText(exportedContents)
                Objects.equals(result.error.reason, "Not Found")
            } catch (ignored) {
                return false
            }
        }, "GeoRocket indexer", "Unexpected chunks for query $query found")
    }

    /**
     * Assert that a chunk with the specified id is found for the specified query
     * @param query the query to search for
     * @param id the id of the chunk which should be found
     */
    def assertFound(def query, def id) {
        testUntilOK({
            def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
                    + "?search=" + URLEncoder.encode(query, 'UTF-8'), null, true)
            def res = checkExportedBuilding(exportedContents, id)
            res == null
        }, "GeoRocket indexer", "No chunk with id $id for query $query")
    }
}
