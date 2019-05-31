package io.georocket.commands

import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import io.vertx.core.Handler
import java.io.PrintWriter

/**
 * Modify properties of existing chunks in the GeoRocket data store
 */
class PropertyCommand : AbstractGeoRocketCommand() {
  override val usageName = "property"
  override val usageDescription =
      "Modify properties of existing chunks in the GeoRocket data store"

  @set:CommandDescList(
      CommandDesc(longName = "get",
          description = "get values of a property",
          command = GetPropertyCommand::class),
      CommandDesc(longName = "set",
          description = "set properties of existing chunks",
          command = SetPropertyCommand::class),
      CommandDesc(longName = "rm",
          description = "remove properties from existing chunks",
          command = RemovePropertyCommand::class))
  var subcommand: AbstractGeoRocketCommand? = null
    set(cmd) {
      field = cmd
      field?.vertx = vertx
      field?.config = config
    }

  override fun checkArguments(): Boolean {
    if (subcommand == null) {
      error("no subcommand given")
      return false
    }
    return super.checkArguments()
  }

  override fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter, handler: Handler<Int>) {
    subcommand!!.endHandler = handler
    subcommand!!.run(remainingArgs, i, o)
  }
}
