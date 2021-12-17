package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.index.Index
import io.georocket.output.MultiMerger
import io.georocket.storage.Store
import io.georocket.util.PathUtils
import io.georocket.util.io.PrintWriteStream
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.FileNotFoundException
import java.io.PrintWriter

/**
 * Searches the GeoRocket data store and outputs the retrieved files
 */
class SearchCommand : DataCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(SearchCommand::class.java)
  }

  private var query: String? = null

  override val usageName = "search"
  override val usageDescription = "Search the GeoRocket data store"

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer to search",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  var layer: String? = null

  /**
   * Enable optimistic merging
   */
  @set:OptionDesc(longName = "optimistic-merging",
      description = "enable optimistic merging")
  var optimisticMerging: Boolean = false

  /**
   * Set the query parts
   */
  @UnknownAttributes("QUERY")
  @Suppress("UNUSED")
  fun setQueryParts(queryParts: List<String>) {
    // put quotes around query parts containing a space
    val quotedQueryParts = queryParts
        .map { if (it.contains(' ')) "\"$it\"" else it }
        .toList()

    // join all query parts using the space character
    this.query = quotedQueryParts.joinToString(" ")
  }

  override fun checkArguments(): Boolean {
    if (query.isNullOrBlank() && layer.isNullOrBlank()) {
      error("no search query and no layer given. Do you really wish to " +
          "export the whole data store? If so, set the layer to `/'.")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      writer: PrintWriter, store: Store, index: Index): Int {
    return try {
      val ws = PrintWriteStream(writer)
      val query = compileQuery(query, layer)
      val metas = index.getMeta(query)

      val merger = MultiMerger(optimisticMerging)

      // skip initialization if optimistic merging is enabled
      if (!optimisticMerging) {
        metas.forEach { merger.init(it.second) }
      }

      var accepted = 0L
      var notaccepted = 0L
      for (chunkMeta in metas) {
        val chunk = store.getOne(chunkMeta.first)
        try {
          merger.merge(chunk, chunkMeta.second, ws)
          accepted++
        } catch (e: IllegalStateException) {
          // Chunk cannot be merged. maybe it's a new one that has
          // been added after the merger was initialized. Just
          // ignore it, but emit a warning later
          notaccepted++
        }
      }

      if (notaccepted > 0) {
        error("could not merge " + notaccepted + " chunks "
            + "because the merger did not accept them. Most likely "
            + "these are new chunks that were added while "
            + "merging was in progress or those that were ignored "
            + "during optimistic merging. If this worries you, "
            + "just repeat the request.")
      }

      if (accepted > 0) {
        merger.finish(ws)
      } else {
        throw FileNotFoundException("Not Found")
      }

      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoSuchElementException &&
          t !is NoStackTraceThrowable) {
        log.error("Could not query store", t)
      }
      1
    }
  }
}
