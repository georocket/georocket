package io.georocket.commands;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.client.GeoRocketClient;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Set properties to existing chunks in the GeoRocket data store
 * @author Benedikt Hiemenz
 */
public class SetPropertyCommand extends AbstractGeoRocketCommand {
  private static Logger log = LoggerFactory.getLogger(SetPropertyCommand.class);

  protected String query;
  protected String layer;
  protected List<String> properties;

  /**
   * Set the query parts
   * @param queryParts the query parts
   */
  @UnknownAttributes("QUERY")
  public void setQueryParts(List<String> queryParts) {
    this.query = String.join(" ", queryParts);
  }

  /**
   * Set the absolute path to the layer from which to update properties
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer containing the chunks whose "
        + "properties should be set",
      argumentName = "PATH", argumentType = Option.ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
  }

  /**
   * The properties to set to the queried chunks within the given layer
   * @param properties the properties
   */
  @OptionDesc(longName = "properties", shortName = "props",
      description = "comma-separated list of properties to set (e.g. "
        + "`key1:value1,key2:value2`)",
      argumentName = "PROPERTIES", argumentType = Option.ArgumentType.STRING)
  public void setProperties(String properties) {
    if (properties == null || properties.isEmpty()) {
      this.properties = null;
    } else {
      this.properties = Stream.of(properties.split(","))
          .map(String::trim)
          .collect(Collectors.toList());
    }
  }

  @Override
  public String getUsageName() {
    return "property set";
  }

  @Override
  public String getUsageDescription() {
    return "Set properties of existing chunks in the GeoRocket data store";
  }

  @Override
  public boolean checkArguments() {
    if (properties == null || properties.isEmpty()) {
      error("no properties given");
      return false;
    }
    return super.checkArguments();
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    GeoRocketClient client = createClient();
    client.getStore().setProperties(query, layer, properties, ar -> {
      if (ar.failed()) {
        client.close();
        Throwable t = ar.cause();
        error(t.getMessage());
        if (!(t instanceof NoStackTraceThrowable)) {
          log.error("Could not set properties", t);
        }
        handler.handle(1);
      } else {
        handler.handle(0);
      }
    });
  }
}
