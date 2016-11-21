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

        assertTrue(exportOk, "Export failed")
    }

    /**
     * Search for buildings in an area where there should be none. Check if
     * GeoRocket really returns no chunks.
     */
    def testExportByBoundingBoxNone() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=12.0,51.0,12.1,51.1", null, true)
        assertEquals(exportedContents, "Not Found", "Response: $exportedContents")
    }

    /**
     * Search for a dummy string. Check if GeoRocket really returns no chunks.
     */
    def testExportNone() {
        def exportedContents = run("curl -sS -X GET http://${georocketHost}:63020/store/"
            + "?search=foobar", null, true)
        assertEquals(exportedContents, "Not Found", "Response: $exportedContents")
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
            if (exportedContents != "Not Found") {
                logWarn("Response: $exportedContents")
            } else {
                deleteOk = true
                break
            }
        }

        assertTrue(deleteOk, "Deleting failed")
    }
}
