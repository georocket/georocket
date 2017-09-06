package io.georocket.commands;

import de.undercouch.underline.CommandDesc;
import de.undercouch.underline.CommandDescList;
import de.undercouch.underline.InputReader;
import de.undercouch.underline.OptionParserException;
import io.vertx.core.Handler;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Update tags of existing chunks in the GeoRocket data store
 * @author Benedikt Hiemenz
 */
public class TagCommand extends AbstractGeoRocketCommand {
  /**
   * The subcommand to run.
   */
  protected AbstractGeoRocketCommand subcommand;

  /**
   * Set the actual command to execute
   * @param subcommand the command
   */
  @CommandDescList({
    @CommandDesc(longName = "add",
        description = "add tags to existing chunks",
        command = AddTagCommand.class),
    @CommandDesc(longName = "rm",
        description = "remove tags from existing chunks",
        command = RemoveTagCommand.class)
  })
  public void setCommand(AbstractGeoRocketCommand subcommand) {
    this.subcommand = subcommand;
    this.subcommand.setVertx(vertx);
    this.subcommand.setConfig(config());
  }

  @Override
  public boolean checkArguments() {
    if (subcommand == null) {
      error("no subcommand given");
      return false;
    }
    return super.checkArguments();
  }

  @Override
  public String getUsageName() {
    return "tag";
  }

  @Override
  public String getUsageDescription() {
    return "Modify tags of existing chunks in the GeoRocket data store";
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    subcommand.setEndHandler(handler);
    subcommand.run(remainingArgs, in, out);
  }
}
