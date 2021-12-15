package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.index.Index
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter

/**
 * Delete chunks or layers from the GeoRocket data store
 */
class DeleteCommand : DataCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(DeleteCommand::class.java)
  }

  private var query: String? = null

  override val usageName = "delete"
  override val usageDescription = "Delete from the GeoRocket data store"

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer from which to delete",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  var layer: String? = null

  /**
   * Set the query parts
   */
  @UnknownAttributes("QUERY")
  @Suppress("UNUSED")
  fun setQueryParts(queryParts: List<String>) {
    query = queryParts.joinToString(" ")
  }

  override fun checkArguments(): Boolean {
    if (query.isNullOrBlank() && layer.isNullOrBlank()) {
      error("no search query and no layer given. Do you really wish to " +
          "delete the whole data store? If so, set the layer to `/'.")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      writer: PrintWriter, store: Store, index: Index): Int {
    return try {
      val query = compileQuery(query, PathUtils.addLeadingSlash(layer ?: ""))
      val paths = index.getPaths(query)
      index.delete(query)
      store.delete(paths)
      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoStackTraceThrowable) {
        log.error("Could not delete from store", t)
      }
      1
    }
  }
}
