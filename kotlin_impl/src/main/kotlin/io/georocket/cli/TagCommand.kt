package io.georocket.cli

import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

/**
 * Update tags of existing chunks in the GeoRocket data store
 */
class TagCommand : GeoRocketCommand() {
  @set:CommandDescList(
      CommandDesc(longName = "add",
          description = "add tags to existing chunks",
          command = AddTagCommand::class),
      CommandDesc(longName = "rm",
          description = "remove tags from existing chunks",
          command = RemoveTagCommand::class))
  var subcommand: GeoRocketCommand? = null

  override fun checkArguments(): Boolean {
    if (subcommand == null) {
      error("no subcommand given")
      return false
    }
    return super.checkArguments()
  }

  override val usageName = "tag"

  override val usageDescription =
      "Modify tags of existing chunks in the GeoRocket data store"

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>): Int {
    return subcommand!!.coRun(remainingArgs, reader, out)
  }
}
