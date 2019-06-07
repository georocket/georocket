package io.georocket.commands

import de.undercouch.underline.CommandDesc
import de.undercouch.underline.CommandDescList
import de.undercouch.underline.InputReader
import java.io.PrintWriter

/**
 * Update tags of existing chunks in the GeoRocket data store
 */
class TagCommand : AbstractGeoRocketCommand() {
  @set:CommandDescList(
      CommandDesc(longName = "add",
          description = "add tags to existing chunks",
          command = AddTagCommand::class),
      CommandDesc(longName = "rm",
          description = "remove tags from existing chunks",
          command = RemoveTagCommand::class))
  var subcommand: AbstractGeoRocketCommand? = null

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

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
    return subcommand!!.runAwait(remainingArgs, i, o)
  }
}
