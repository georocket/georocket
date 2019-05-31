package io.georocket.commands

import io.georocket.client.SearchParams
import io.vertx.core.Handler
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter
import java.util.NoSuchElementException

/**
 * Abstract base class for commands that need to export data
 */
abstract class AbstractQueryCommand : AbstractGeoRocketCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(AbstractQueryCommand::class.java)
  }

  /**
   * Query data store using a search query and a layer
   * @param query the search query (may be `null`)
   * @param layer the layer to export (may be `null`)
   * @param out the writer to write the results to
   * @param handler the handler that should be called when all  chunks have
   * been exported
   */
  protected fun query(query: String, layer: String, out: PrintWriter,
      handler: Handler<Int>) {
    query(SearchParams().setQuery(query).setLayer(layer), out, handler)
  }

  /**
   * Query data store using a search query and a layer
   * @param params search parameters
   * @param out the writer to write the results to
   * @param handler the handler that should be called when all
   * chunks have been exported
   */
  protected fun query(params: SearchParams, out: PrintWriter,
      handler: Handler<Int>) {
    val client = createClient()
    client.store.search(params) { ar ->
      if (ar.failed()) {
        error(ar.cause().message)
        if (ar.cause() !is NoSuchElementException &&
            ar.cause() !is NoStackTraceThrowable) {
          log.error("Could not query store", ar.cause())
        }
        handler.handle(1)
      } else {
        val sr = ar.result()
        val r = sr.response
        r.handler { out.write(it.toString()) }
        r.endHandlerWithResult { srsr ->
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
          client.close()
          handler.handle(0)
        }
      }
    }
  }
}
