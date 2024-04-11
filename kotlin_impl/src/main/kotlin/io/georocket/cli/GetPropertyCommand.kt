package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.index.Index
import io.georocket.storage.Store
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.NoStackTraceThrowable
import io.vertx.core.streams.WriteStream
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory

/**
 * Get all values of a property
 */
class GetPropertyCommand : DataCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(GetPropertyCommand::class.java)
  }

  private var query: String? = null

  override val usageName = "property get"
  override val usageDescription = "Get all values of a property"

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks whose " +
          "property values should be retrieved",
      argumentName = "PATH", argumentType = Option.ArgumentType.STRING)
  var layer: String? = null

  @set:OptionDesc(longName = "property", shortName = "prop",
      description = "the name of the property",
      argumentName = "PROPERTIES", argumentType = Option.ArgumentType.STRING)
  var property: String? = null

  /**
   * Set the query parts
   */
  @UnknownAttributes("QUERY")
  @Suppress("UNUSED")
  fun setQueryParts(queryParts: List<String>) {
    this.query = queryParts.joinToString(" ")
  }

  override fun checkArguments(): Boolean {
    if (property.isNullOrBlank()) {
      error("no property given")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>, store: Store, index: Index): Int {
    return try {
      val query = compileQuery(query, layer)
      val r = index.getPropertyValues(query, property!!)
      r.collect { value ->
        out.write(Buffer.buffer(value.toString() + "\n")).await()
      }
      0
    } catch (t: Throwable) {
      error(t.message)
      if (t !is NoStackTraceThrowable) {
        log.error("Could not get values of property $property", t)
      }
      1
    }
  }
}
