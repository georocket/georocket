package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.index.Index
import io.georocket.storage.Store
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.streams.WriteStream
import io.vertx.kotlin.coroutines.await
import org.fusesource.jansi.AnsiConsole
import org.slf4j.LoggerFactory

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
      out: WriteStream<Buffer>, store: Store, index: Index): Int {
    AnsiConsole.systemInstall()
    return try {
      val deleted = SpinnerRenderer(vertx, "Deleting").use {
        val query = compileQuery(query, layer)
        val paths = index.getPaths(query)
        val d = store.delete(paths)
        index.delete(query)
        d
      }
      out.write(Buffer.buffer("Successfully deleted $deleted chunks.\n")).await()
      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoStackTraceThrowable) {
        log.error("Could not delete from store", t)
      }
      1
    } finally {
      AnsiConsole.systemUninstall()
    }
  }
}
