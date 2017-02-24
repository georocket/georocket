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
        assertTrue(exportedContents.length() >= 100, "Response: $exportedContents")

        // parse exported file
        def exportedNode = new XmlSlurper().parseText(exportedContents)

        // check if we have only exported one chunk
        assertEquals(exportedNode.children().size(), 1,
            "Expected 1 chunks. Got ${exportedNode.children().size()}.")

        // check if we found the right building
        def child = exportedNode.children().getAt(0)
        def gmlId = child.Building.'@gml:id'
        assertEquals(gmlId, expectedId, "Expected gml:id ${expectedId}. Got ${gmlId}.")
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
}
