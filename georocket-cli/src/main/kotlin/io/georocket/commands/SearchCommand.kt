package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.client.SearchParams
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter

/**
 * Searches the GeoRocket data store and outputs the retrieved files
 */
class SearchCommand : AbstractQueryCommand() {
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
    if (query == null || query!!.isEmpty()) {
      error("no search query given")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
    val params = SearchParams()
        .setQuery(query)
        .setLayer(layer)
        .setOptimisticMerging(optimisticMerging)
    return try {
      query(params, o)
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
