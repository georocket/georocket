package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.index.Index
import io.georocket.index.PropertiesParser
import io.georocket.storage.Store
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.streams.WriteStream
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.slf4j.LoggerFactory

/**
 * Set properties to existing chunks in the GeoRocket data store
 */
class SetPropertyCommand : DataCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(SetPropertyCommand::class.java)
  }

  override val usageName = "property set"
  override val usageDescription =
      "Set properties of existing chunks in the GeoRocket data store"

  private var query: String? = null

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks whose " +
          "properties should be set",
      argumentName = "PATH", argumentType = Option.ArgumentType.STRING)
  var layer: String? = null

  @set:OptionDesc(longName = "properties", shortName = "props",
    description = "comma-separated list of properties to set (e.g. " +
        "`key1:value1,key2:value2`)",
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
      PropertiesParser.parse(properties)
    } catch (e: ParseCancellationException) {
      error("Invalid property syntax: ${e.message}")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>, store: Store, index: Index): Int {
    return try {
      val query = compileQuery(query, layer)
      val props = PropertiesParser.parse(properties)
      index.setProperties(query, props)
      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoStackTraceThrowable) {
        log.error("Could not set properties", t)
      }
      1
    }
  }
}
