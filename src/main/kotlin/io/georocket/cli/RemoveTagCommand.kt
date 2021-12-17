package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.index.Index
import io.georocket.index.TagsParser
import io.georocket.storage.Store
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import org.antlr.v4.runtime.misc.ParseCancellationException
import java.io.PrintWriter

/**
 * Remove tags from existing chunks in the GeoRocket data store
 */
class RemoveTagCommand : DataCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(RemoveTagCommand::class.java)
  }

  private var query: String? = null

  override val usageName = "tag rm"
  override val usageDescription =
      "Remove tags from existing chunks in the GeoRocket data store"

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks from " +
          "which the given tags should be removed",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  var layer: String? = null

  @set:OptionDesc(longName = "tags", shortName = "t",
    description = "comma-separated list of tags to remove from the chunks",
    argumentName = "TAGS", argumentType = ArgumentType.STRING)
  var tags: String? = null

  /**
   * Set the query parts
   */
  @UnknownAttributes("QUERY")
  @Suppress("UNUSED")
  fun setQueryParts(queryParts: List<String>) {
    this.query = queryParts.joinToString(" ")
  }

  override fun checkArguments(): Boolean {
    if (tags.isNullOrEmpty()) {
      error("no tags given")
      return false
    }
    try {
      TagsParser.parse(tags)
    } catch (e: ParseCancellationException) {
      error("Invalid tag syntax: ${e.message}")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      writer: PrintWriter, store: Store, index: Index): Int {
    return try {
      val query = compileQuery(query, layer)
      val ts = TagsParser.parse(tags)
      index.removeTags(query, ts)
      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoStackTraceThrowable) {
        log.error("Could not remove the tags", t)
      }
      1
    }
  }
}
