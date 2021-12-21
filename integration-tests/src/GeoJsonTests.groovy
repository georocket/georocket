import groovy.json.JsonException

import static Utils.*
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Tests if GeoRocket can handle GeoJSON files correctly
 * @author Michel Kraemer
 */
class GeoJsonTests extends StoreTests {
    private String georocketHost

    static EXPECTED_CONTENTS = new File("/data/featurecollection.json").text
    static EXPECTED_FEATURE_COLL = new JsonSlurper().parseText(EXPECTED_CONTENTS)

    /**
     * Creates the test class
     * @param georocketHost the host where GeoRocket is running
     */
    GeoJsonTests(String georocketHost) {
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
        def exportedFeatureColl = new JsonSlurper().parseText(exportedContents)

        // compare number of features
        def expectedCount = EXPECTED_FEATURE_COLL.features.size()
        if (exportedFeatureColl.type == 'Feature') {
            logWarn("Expected ${expectedCount} chunks. Got 1.")
            return false
        } else {
            def exportedCount = exportedFeatureColl.features.size()
            if (exportedCount < expectedCount) {
                logWarn("Expected ${expectedCount} chunks. Got ${exportedCount}.")
                return false
            } else if (exportedCount > expectedCount) {
                fail("Expected ${expectedCount} chunks. Got ${exportedCount}.")
            }
        }

        // compare features
        for (exportedFeature in exportedFeatureColl.features) {
            def expectedFeature = EXPECTED_FEATURE_COLL.features.find {
                it.properties.name == exportedFeature.properties.name }
            String exportedStr = JsonOutput.toJson(exportedFeature)
            String expectedStr = JsonOutput.toJson(expectedFeature)
            assertEquals(exportedStr.trim(), expectedStr.trim(),
                "Exported feature does not match expected one")
        }

        return true
    }

    /**
     * Import a GeoJSON file into the GeoRocket store, wait until it has been
     * indexed and then check if the store's contents are as expected
     */
    def testImport() {
        // import file
        run("curl -sS -X POST http://${georocketHost}:63020/store "
            + "--data @data/featurecollection.json")
        testExportUntilOK()
    }

    /**
     * Assert that the given string contains exactly one feature with the given name
     * @param exportedContents the string to parse
     * @param expectedId the expected feature name
     */
    private void assertExportedFeature(exportedContents, expectedName) {
        assertTrue(exportedContents.length() >= 100, "Response: $exportedContents")

        // parse exported file
        def exportedFeature = new JsonSlurper().parseText(exportedContents)

        // check if we have only exported one feature
        assertEquals("FeatureCollection", exportedFeature.type,
            "Expected a feature collection. Got ${exportedFeature.type}.")
        assertEquals(1, exportedFeature.features.size(),
            "Expected 1 feature. Got ${exportedFeature.features.size()}.")

        // check if we found the right feature
        def name = exportedFeature.features[0].properties.name
        assertEquals(name, expectedName, "Expected name ${expectedName}. Got ${name}.")
    }

    /**
     * Search for a features using a bounding box. Check if GeoRocket really
     * exports only one feature and if it is the right one.
     */
    def testExportByBoundingBox() {
        // export file using bounding box that relates to the one of the features
        // we are looking for
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=8.6598,49.87423,8.6600,49.87426",
            null, true)
        assertExportedFeature(exportedContents, 'Fraunhofer IGD')
    }

    /**
     * Search for a feature by a property. Check if GeoRocket really
     * exports only one feature and if it is the right one.
     */
    def testExportByProperty() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=Darmstadtium", null, true)
        assertExportedFeature(exportedContents, 'Darmstadtium')
    }

    /**
     * Search for a feature by key-value pair. Check if GeoRocket really
     * exports only one feature and if it is the right one.
     */
    def testExportByKeyValue() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=" + URLEncoder.encode('EQ(name "Fraunhofer IGD")', 'UTF-8'), null, true)
        assertExportedFeature(exportedContents, 'Fraunhofer IGD')
    }
}
