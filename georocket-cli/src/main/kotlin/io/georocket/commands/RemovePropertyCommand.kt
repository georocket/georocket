package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.util.coroutines.removeProperties
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.logging.LoggerFactory
import java.io.PrintWriter

/**
 * Remove properties from existing chunks in the GeoRocket data store
 */
class RemovePropertyCommand : AbstractGeoRocketCommand() {
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
  private var properties: List<String>? = null

  /**
   * Set the query parts
   */
  @UnknownAttributes("QUERY")
  @Suppress("UNUSED")
  fun setQueryParts(queryParts: List<String>) {
    this.query = queryParts.joinToString(" ")
  }

  /**
   * Set the properties to remove from the queried chunks within the given layer
   */
  @OptionDesc(longName = "properties", shortName = "props",
      description = "comma-separated list of property keys to remove from the chunks",
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
        client.store.removeProperties(query, layer, properties)
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
}
