package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option
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
 * Remove properties from existing chunks in the GeoRocket data store
 */
class RemovePropertyCommand : DataCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(RemovePropertyCommand::class.java)
  }

  override val usageName = "property rm"
  override val usageDescription =
      "Remove properties from existing chunks in the GeoRocket data store"

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks from " +
          "which the properties should be removed",
      argumentName = "PATH", argumentType = Option.ArgumentType.STRING)
  var layer: String? = null

  private var query: String? = null

  @set:OptionDesc(longName = "properties", shortName = "props",
    description = "comma-separated list of property keys to remove from the chunks",
    argumentName = "PROPERTIES", argumentType = Option.ArgumentType.STRING)
  var properties: String? = null

  /**
   * Set the query parts
   */
  @UnknownAttributes("QUERY")
  @Suppress("UNUSED")
  fun setQueryParts(queryParts: List<String>) {
    this.query = queryParts.joinToString(" ")
  }

  override fun checkArguments(): Boolean {
    if (properties.isNullOrEmpty()) {
      error("no properties given")
      return false
    }
    try {
      TagsParser.parse(properties)
    } catch (e: ParseCancellationException) {
      error("Invalid property syntax: ${e.message}")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      writer: PrintWriter, store: Store, index: Index): Int {
    return try {
      val query = compileQuery(query, layer)
      val propNames = TagsParser.parse(properties)
      index.removeProperties(query, propNames)
      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoStackTraceThrowable) {
        log.error("Could not remove properties", t)
      }
      1
    }
  }
}
