package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.client.SearchParams
import io.vertx.core.Handler
import java.io.PrintWriter

/**
 * Exports a layer or the whole data store
 */
class ExportCommand : AbstractQueryCommand() {
  private var layer: String? = null

  override val usageName = "export"
  override val usageDescription = "Export a layer or the whole data store"

  @set:OptionDesc(longName = "optimistic-merging",
      description = "enable optimistic merging")
  var optimisticMerging: Boolean = false

  /**
   * Set the absolute path to the layer to export
   * @param layer the layer
   */
  @UnknownAttributes("LAYER")
  @Suppress("UNUSED")
  fun setLayer(layer: List<String>) {
    var l = layer.joinToString(" ").trim()
    if (l.isNotEmpty()) {
      if (!l.endsWith("/")) {
        l = "$l/"
      }
      if (!l.startsWith("/")) {
        l = "/$l"
      }
    }
    this.layer = l
  }

  override fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter, handler: Handler<Int>) {
    val params = SearchParams()
        .setLayer(layer)
        .setOptimisticMerging(optimisticMerging)
    query(params, o, handler)
  }
}
