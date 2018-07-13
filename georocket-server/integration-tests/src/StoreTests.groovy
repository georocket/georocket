import groovy.json.JsonException
import groovy.json.JsonSlurper

import static Utils.*

abstract class StoreTests {
    /**
     * Export the whole data store of GeoRocket to a file and check if the
     * contents are as expected
     */
    abstract def testExport()

    /**
     * Call #testExport() until it succeeds or fail after several tries
     */
    protected def testExportUntilOK() {
        testUntilOK({ testExport() }, "GeoRocket indexer", "Export failed")
    }

    /**
     * Call a test method until it succeeds or fails after several tries
     * @param test the test method, returns true if test test succeeds,
     * false otherwise
     * @param waitFor the name of the component to wait for
     * @param errorMessage the error message if the test does not succeed
     * @param waitTime the time to wait before retry 
     * @param waitIterations the number times the test should be tried
     */
    protected def testUntilOK(test, String waitFor, String errorMessage,
            int waitTime = 2000, int waitIterations = 90) {
        for (int i = 0; i < waitIterations; ++i) {
            if (test()) {
                return
            }
            
            logWait(waitFor)
            Thread.sleep(waitTime)
        }
        
        fail(errorMessage)
    }

    /**
     * Search for buildings in an area where there should be none. Check if
     * GeoRocket really returns no chunks.
     */
    def testExportByBoundingBoxNone() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=12.0,51.0,12.1,51.1", null, true)
        def result = new JsonSlurper().parseText(exportedContents)
        assertEquals(result.error.reason, "Not Found", "Response: $exportedContents")
    }

    /**
     * Search for buildings by key-value pairs that do not exist
     */
    def testExportByKeyValueNone() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=" + URLEncoder.encode('EQ(foo bar)', 'UTF-8'), null, true)
        def result = new JsonSlurper().parseText(exportedContents)
        assertEquals(result.error.reason, "Not Found", "Response: $exportedContents")
    }

    /**
     * Search for a dummy string. Check if GeoRocket really returns no chunks.
     */
    def testExportNone() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=foobar", null, true)
        def result = new JsonSlurper().parseText(exportedContents)
        assertEquals(result.error.reason, "Not Found", "Response: $exportedContents")
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
            def exportedContents = run("curl -sS -X GET "
                + "http://${georocketHost}:63020/store/", null, true)

            def result = new JsonSlurper().parseText(exportedContents)
          
            if (result.error && result.error.reason != "Not Found") {
                logWarn("Response: $exportedContents")
            } else {
                deleteOk = true
                break
            }
        }

        assertTrue(deleteOk, "Deleting failed")
    }
}
