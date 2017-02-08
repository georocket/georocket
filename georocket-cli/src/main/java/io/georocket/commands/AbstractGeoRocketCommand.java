package io.georocket.commands;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;

import de.undercouch.underline.Command;
import de.undercouch.underline.InputReader;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionGroup;
import de.undercouch.underline.OptionIntrospector;
import de.undercouch.underline.OptionIntrospector.ID;
import de.undercouch.underline.OptionParser;
import de.undercouch.underline.OptionParserException;
import io.georocket.ConfigConstants;
import io.georocket.client.GeoRocketClient;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Abstract base class for all GeoRocket commands
 * @author Michel Kraemer
 */
public abstract class AbstractGeoRocketCommand implements GeoRocketCommand {
  private OptionGroup<ID> options;
  private boolean displayHelp;
  protected Vertx vertx;
  private JsonObject config;
  private Handler<Integer> endHandler;
  
  /**
   * Set the Vert.x instance
   * @param vertx the instance
   */
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }
  
  /**
   * @return the configuration object
   */
  protected JsonObject config() {
    return config;
  }
  
  /**
   * Set the configuration object
   * @param config the configuration object
   */
  public void setConfig(JsonObject config) {
    this.config = config;
  }
  
  /**
   * Set the end handler that will be called when the command has finished its work
   * @param endHandler the end handler
   */
  public void setEndHandler(Handler<Integer> endHandler) {
    this.endHandler = endHandler;
  }
  
  /**
   * Specifies if the command's help should be displayed
   * @param display true if the help should be displayed
   */
  @OptionDesc(longName = "help", shortName = "h",
      description = "display this help and exit", priority = 9000)
  public void setDisplayHelp(boolean display) {
    this.displayHelp = display;
  }
  
  /**
   * Outputs an error message
   * @param msg the message
   */
  protected void error(String msg) {
    System.err.println("georocket: " + msg);
  }
  
  /**
   * @return the classes to inspect for CLI options
   */
  protected Class<?>[] getClassesToIntrospect() {
    return new Class<?>[] { getClass() };
  }
  
  /**
   * @return the commands the parsed CLI values should be injected into
   */
  protected Command[] getObjectsToEvaluate() {
    return new Command[] { this };
  }
  
  @Override
  public int run(String[] args, InputReader in, PrintWriter out)
      throws OptionParserException, IOException {
    if (options == null) {
      try {
        options = OptionIntrospector.introspect(getClassesToIntrospect());
      } catch (IntrospectionException e) {
        throw new RuntimeException("Could not inspect command", e);
      }
    }
    
    boolean unknownArgs = OptionIntrospector.hasUnknownArguments(
        getClassesToIntrospect());
    OptionParser.Result<ID> parsedOptions = OptionParser.parse(args,
        options, unknownArgs ? OptionIntrospector.DEFAULT_ID : null);
    try {
      OptionIntrospector.evaluate(parsedOptions.getValues(),
          (Object[])getObjectsToEvaluate());
    } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
      throw new RuntimeException("Could not evaluate options", e);
    }
    
    if (displayHelp) {
      usage();
      endHandler.handle(0);
      return 0;
    }
    
    if (!checkArguments()) {
      endHandler.handle(1);
      return 1;
    }
    
    doRun(parsedOptions.getRemainingArgs(), in, out, endHandler);
    return 0;
  }
  
  /**
   * Prints out usage information
   */
  protected void usage() {
    String name = "georocket";
    
    String footnotes = null;
    if (!options.getCommands().isEmpty()) {
      footnotes = "Use `" + name + " help <command>' to read about a specific command.";
    }
    
    String usageName = getUsageName();
    if (usageName != null && !usageName.isEmpty()) {
      name += " " + usageName;
    }
    
    String unknownArguments = OptionIntrospector.getUnknownArgumentName(
        getClassesToIntrospect());
    
    OptionParser.usage(name, getUsageDescription(), options,
        unknownArguments, footnotes, new PrintWriter(System.out, true));
  }
  
  @Override
  public boolean checkArguments() {
    // nothing to check by default. subclasses may override
    return true;
  }
  
  /**
   * Create a new GeoRocket client
   * @return the client
   */
  protected GeoRocketClient createClient() {
    String host = config().getString(ConfigConstants.HOST);
    int port = config().getInteger(ConfigConstants.PORT);
    return new GeoRocketClient(host, port, vertx);
  }
}
