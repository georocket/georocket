package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;

import de.undercouch.underline.Command;
import de.undercouch.underline.InputReader;
import de.undercouch.underline.OptionParserException;
import io.vertx.core.Handler;

/**
 * An interface for all GeoRocket commands
 * @author Michel Kraemer
 */
public interface GeoRocketCommand extends Command {
  /**
   * @return the command's name displayed in the help
   */
  String getUsageName();
  
  /**
   * @return the command description that should be displayed in the help
   */
  String getUsageDescription();

  /**
   * Checks the provided arguments
   * @return true if all arguments are OK, false otherwise
   */
  boolean checkArguments();
  
  /**
   * Runs the command
   * @param remainingArgs arguments that have not been parsed yet, can
   * be forwarded to sub-commands
   * @param in a stream from which user input can be read
   * @param out a stream to write the output to
   * @param handler has to be called when the command has finished its work
   * @throws OptionParserException if the remaining arguments could not be parsed
   * @throws IOException if input files could not be read or the output
   * stream could not be written
   */
  void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException;
}
