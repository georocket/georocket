package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.util.coroutines.delete
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter

/**
 * Delete chunks or layers from the GeoRocket data store
 */
class DeleteCommand : AbstractQueryCommand() {
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
    this.query = queryParts.joinToString(" ")
  }

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
    return createClient().use { client ->
      try {
        client.store.delete(query, layer)
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
}
