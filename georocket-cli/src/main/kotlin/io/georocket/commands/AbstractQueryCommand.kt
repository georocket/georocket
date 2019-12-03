package io.georocket.commands

import io.georocket.client.SearchParams
import io.georocket.util.coroutines.search
import kotlinx.coroutines.delay
import java.io.PrintWriter

/**
 * Abstract base class for commands that need to export data
 */
abstract class AbstractQueryCommand : AbstractGeoRocketCommand() {
  /**
   * Query data store using a search query and a layer
   * @param query the search query (may be `null`)
   * @param layer the layer to export (may be `null`)
   * @param out the writer to write the results to
   */
  protected suspend fun query(query: String, layer: String, out: PrintWriter) {
    query(SearchParams().setQuery(query).setLayer(layer), out)
  }

  /**
   * Query data store using a search query and a layer
   * @param params search parameters
   * @param out the writer to write the results to
   */
  protected suspend fun query(params: SearchParams, out: PrintWriter) {
    createClient().use { client ->
      val r = client.store.search(params)
      for (buf in r) {
        out.write(buf.toString())
      }

      val srsr = r.result
      if (srsr.unmergedChunks > 0) {
        System.err.println("${srsr.unmergedChunks} chunks could not " +
            "be merged. This usually has one of the following causes:\n" +
            "\n" +
            "* Chunks were added to GeoRocket's store while merging was " +
            "in progress\n" +
            "* Optimistic merging was enabled and some chunks did not fit " +
            "to the search\n" +
            "  result\n")
        if (params.isOptimisticMerging) {
          System.err.println("Optimistic merging was enabled. Repeat the " +
              "request with optimistic merging\n" +
              "disabled if you want to get all chunks.")
        } else {
          System.err.println("Repeat the request if you want to get all chunks.")
        }
      }
    }
  }
}
