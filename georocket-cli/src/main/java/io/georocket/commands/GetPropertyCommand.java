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

/**
 * Get all values of a property
 * @author Tim Hellhake
 */
public class GetPropertyCommand extends AbstractGeoRocketCommand {
  private static Logger log = LoggerFactory.getLogger(GetPropertyCommand.class);

  protected String query;
  protected String layer;
  private String property;

  /**
   * Set the query parts
   * @param queryParts the query parts
   */
  @UnknownAttributes("QUERY")
  public void setQueryParts(List<String> queryParts) {
    this.query = String.join(" ", queryParts);
  }

  /**
   * Set the absolute path to the layer from which to update property
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
    description = "absolute path to the layer in which to set property",
    argumentName = "PATH", argumentType = Option.ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
  }

  /**
   * The property to set to the queried chunks within the given layer
   * @param property the property
   */
  @OptionDesc(longName = "property", shortName = "prop",
    description = "the name of the properties",
    argumentName = "PROPERTIES", argumentType = Option.ArgumentType.STRING)
  public void setProperty(String property) {
    this.property = property;
  }

  @Override
  public String getUsageName() {
    return "property get";
  }

  @Override
  public String getUsageDescription() {
    return "Get all values of a property";
  }

  @Override
  public boolean checkArguments() {
    if (property == null || property.isEmpty()) {
      error("no property given");
      return false;
    }
    return super.checkArguments();
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
    Handler<Integer> handler) throws OptionParserException, IOException {
    GeoRocketClient client = createClient();
    client.getStore().getPropertyValues(property, query, layer, ar -> {
      if (ar.failed()) {
        Throwable t = ar.cause();
        error(t.getMessage());
        if (!(t instanceof NoStackTraceThrowable)) {
          log.error("Could not get values for property " + property, t);
        }
        handler.handle(1);
      } else {
        ar.result().handler(buf -> {
          out.write(buf.toString("utf-8"));
        });
        ar.result().endHandler(v -> {
          client.close();
          handler.handle(0);
        });
      }
    });
  }
}
