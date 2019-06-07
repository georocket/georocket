package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.util.coroutines.setProperties
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter

/**
 * Set properties to existing chunks in the GeoRocket data store
 */
class SetPropertyCommand : AbstractGeoRocketCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(SetPropertyCommand::class.java)
  }

  override val usageName = "property set"
  override val usageDescription =
      "Set properties of existing chunks in the GeoRocket data store"

  private var query: String? = null
  private var properties: List<String>? = null

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks whose " +
          "properties should be set",
      argumentName = "PATH", argumentType = Option.ArgumentType.STRING)
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
   * The properties to set to the queried chunks within the given layer
   */
  @OptionDesc(longName = "properties", shortName = "props",
      description = "comma-separated list of properties to set (e.g. " +
          "`key1:value1,key2:value2`)",
      argumentName = "PROPERTIES", argumentType = Option.ArgumentType.STRING)
  @Suppress("UNUSED")
  fun setProperties(properties: String?) {
    if (properties == null || properties.isEmpty()) {
      this.properties = null
    } else {
      this.properties = properties.split(",")
          .map { it.trim() }
          .filter { it.isNotEmpty() }
          .toList()
    }
  }

  override fun checkArguments(): Boolean {
    if (properties == null || properties!!.isEmpty()) {
      error("no properties given")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
    return createClient().use { client ->
      try {
        client.store.setProperties(query, layer, properties)
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
}
