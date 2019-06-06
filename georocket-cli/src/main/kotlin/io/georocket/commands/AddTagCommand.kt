package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.util.coroutines.appendTags
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter

/**
 * Add tags to existing chunks in the GeoRocket data store
 */
class AddTagCommand : AbstractQueryCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(AddTagCommand::class.java)
  }

  private var query: String? = null
  private var tags: List<String>? = null

  override val usageName = "tag add"
  override val usageDescription =
      "Add tags to existing chunks in the GeoRocket data store"

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks to "
          + "which the tags should be added",
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

  /**
   * Set the tags to append to the queried chunks within the given layer
   */
  @OptionDesc(longName = "tags", shortName = "t",
      description = "comma-separated list of tags to add to the chunks",
      argumentName = "TAGS", argumentType = ArgumentType.STRING)
  @Suppress("UNUSED")
  fun setTags(tags: String?) {
    if (tags == null || tags.isEmpty()) {
      this.tags = null
    } else {
      this.tags = tags.split(",")
          .map { it.trim() }
          .filter { it.isNotEmpty() }
          .toList()
    }
  }

  override fun checkArguments(): Boolean {
    if (tags == null || tags!!.isEmpty()) {
      error("no tags given")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
    return createClient().use { client ->
      try {
        client.store.appendTags(query, layer, tags)
        0
      } catch (t: Throwable) {
        error(t.message)
        if (t !is NoStackTraceThrowable) {
          log.error("Could not add the tags", t)
        }
        1
      }
    }
  }
}
