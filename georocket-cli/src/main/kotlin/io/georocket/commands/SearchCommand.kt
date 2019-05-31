package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.client.SearchParams
import io.vertx.core.Handler
import java.io.PrintWriter

/**
 * Searches the GeoRocket data store and outputs the retrieved files
 */
class SearchCommand : AbstractQueryCommand() {
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

  override fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter, handler: Handler<Int>) {
    val params = SearchParams()
        .setQuery(query)
        .setLayer(layer)
        .setOptimisticMerging(optimisticMerging)
    query(params, o, handler)
  }
}
