package io.georocket;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import de.undercouch.underline.CommandDesc;
import de.undercouch.underline.CommandDescList;
import de.undercouch.underline.InputReader;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.StandardInputReader;
import io.georocket.client.GeoRocketClient;
import io.georocket.commands.AbstractGeoRocketCommand;
import io.georocket.commands.DeleteCommand;
import io.georocket.commands.ExportCommand;
import io.georocket.commands.HelpCommand;
import io.georocket.commands.ImportCommand;
import io.georocket.commands.SearchCommand;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

/**
 * GeoRocket command-line interface
 * @author Michel Kraemer
 */
public class GeoRocketCli extends AbstractGeoRocketCommand {
  private static File geoRocketCliHome;
  
  private boolean displayVersion;
  private AbstractGeoRocketCommand command;
  
  /**
   * The Vert.x instance. Use {@link #getVertx()} to access it.
   */
  private Vertx vertx;
  
  /**
   * Get or create the Vert.x instance
   * @return the Vert.x instance
   */
  private Vertx getVertx() {
    if (vertx == null) {
      vertx = Vertx.vertx();
    }
    return vertx;
  }
  
  @Override
  protected JsonObject config() {
    JsonObject config = super.config();
    if (config == null || config.isEmpty()) {
      // load configuration file
      File confDir = new File(geoRocketCliHome, "conf");
      File confFile = new File(confDir, "georocket.json");
      config = new JsonObject();
      try {
        String confFileStr = FileUtils.readFileToString(confFile, "UTF-8");
        config = new JsonObject(confFileStr);
      } catch (IOException e) {
        System.err.println("Could not read config file " + confFile + ":" + e.getMessage());
      } catch (DecodeException e) {
        System.err.println("Invalid config file: " + e.getMessage());
      }
      
      // set default values
      if (!config.containsKey(ConfigConstants.HOST)) {
        config.put(ConfigConstants.HOST, GeoRocketClient.DEFAULT_HOST);
      }
      if (!config.containsKey(ConfigConstants.PORT)) {
        config.put(ConfigConstants.PORT, GeoRocketClient.DEFAULT_PORT);
      }
      
      setConfig(config);
    }
    return config;
  }
  
  /**
   * Specify if version information should be displayed
   * @param display true if the version should be displayed
   */
  @OptionDesc(longName = "version", shortName = "V",
      description = "output version information and exit",
      priority = 9999)
  public void setDisplayVersion(boolean display) {
    this.displayVersion = display;
  }
  
  /**
   * Set the command to execute
   * @param command the command
   */
  @CommandDescList({
    @CommandDesc(longName = "import",
        description = "import one or more files into GeoRocket",
        command = ImportCommand.class),
    @CommandDesc(longName = "export",
        description = "export from GeoRocket",
        command = ExportCommand.class),
    @CommandDesc(longName = "search",
        description = "search the GeoRocket data store",
        command = SearchCommand.class),
    @CommandDesc(longName = "delete",
        description = "delete from the GeoRocket data store",
        command = DeleteCommand.class),
    @CommandDesc(longName = "help",
        description = "display help for a given command",
        command = HelpCommand.class)
  })
  public void setCommand(AbstractGeoRocketCommand command) {
    this.command = command;
    this.command.setVertx(getVertx());
    this.command.setConfig(config());
  }
  
  /**
   * Run the GeoRocket command-line interface
   * @param args the command line arguments
   * @throws IOException if a stream could not be read
   */
  public static void main(String[] args) throws IOException {
    // get GEOROCKET_CLI_HOME
    String geoRocketCliHomeStr = System.getenv("GEOROCKET_CLI_HOME");
    if (geoRocketCliHomeStr == null) {
      System.err.println("Environment variable GEOROCKET_CLI_HOME not set. "
          + "Using current working directory.");
      geoRocketCliHomeStr = new File(".").getAbsolutePath();
    }
    try {
      geoRocketCliHome = new File(geoRocketCliHomeStr).getCanonicalFile();
    } catch (IOException e) {
      System.err.println("Invalid GeoRocket home: " + geoRocketCliHomeStr);
      System.exit(1);
      return;
    }
    
    // start CLI
    GeoRocketCli cli = new GeoRocketCli();
    try {
      PrintWriter out = new PrintWriter(new OutputStreamWriter(
          System.out, StandardCharsets.UTF_8));
      cli.setEndHandler(exitCode -> {
        out.flush();
        System.exit(exitCode);
      });
      cli.run(args, new StandardInputReader(), out);
    } catch (OptionParserException e) {
      cli.error(e.getMessage());
      System.exit(1);
    }
  }

  @Override
  public String getUsageName() {
    return ""; // the tool's name will be prepended
  }

  @Override
  public String getUsageDescription() {
    return "Command-line interface for GeoRocket";
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    if (displayVersion) {
      version();
      handler.handle(0);
      return;
    }
    
    // if there are no commands print usage and exit
    if (command == null) {
      usage();
      handler.handle(0);
      return;
    }
    
    command.setEndHandler(handler);
    command.run(remainingArgs, in, out);
  }
  
  /**
   * Prints out version information
   */
  private void version() {
    System.out.println("georocket " + getVersion());
  }
  
  /**
   * @return the tool's version string
   */
  public static String getVersion() {
    URL u = GeoRocketCli.class.getResource("version.dat");
    String version;
    try {
      version = IOUtils.toString(u, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Could not read version information", e);
    }
    return version;
  }
}
