package io.georocket.cli

import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 * Modify properties of existing chunks in the GeoRocket data store
 */
class PropertyCommand : GeoRocketCommand() {
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
  var subcommand: GeoRocketCommand? = null

  override fun checkArguments(): Boolean {
    if (subcommand == null) {
      error("no subcommand given")
      return false
    }
    return super.checkArguments()
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>): Int {
    return subcommand!!.coRun(remainingArgs, reader, out)
  }
}
