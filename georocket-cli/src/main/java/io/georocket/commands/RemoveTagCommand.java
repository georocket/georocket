package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.client.GeoRocketClient;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Remove tags from existing chunks in the GeoRocket data store
 * @author Benedikt Hiemenz
 */
public class RemoveTagCommand extends AbstractQueryCommand {
  private static Logger log = LoggerFactory.getLogger(RemoveTagCommand.class);

  protected String query;
  protected String layer;
  protected List<String> tags;

  /**
   * Set the query parts
   * @param queryParts the query parts
   */
  @UnknownAttributes("QUERY")
  public void setQueryParts(List<String> queryParts) {
    this.query = String.join(" ", queryParts);
  }

  /**
   * Set the absolute path to the layer from which to update tags
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer from which to remove tags",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
  }

  /**
   * Set the tags to remove from the queried chunks within the given layer
   * @param tags the tags
   */
  @OptionDesc(longName = "tags", shortName = "t",
      description = "comma-separated list of tags to remove from the chunks",
      argumentName = "TAGS", argumentType = ArgumentType.STRING)
  public void setTags(String tags) {
    if (tags == null || tags.isEmpty()) {
      this.tags = null;
    } else {
      this.tags = Stream.of(tags.split(","))
          .map(String::trim)
          .collect(Collectors.toList());
    }
  }

  @Override
  public String getUsageName() {
    return "tag rm";
  }

  @Override
  public String getUsageDescription() {
    return "Remove tags from existing chunks in the GeoRocket data store";
  }

  @Override
  public boolean checkArguments() {
    if (tags == null || tags.isEmpty()) {
      error("no tags given");
      return false;
    }
    return super.checkArguments();
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    GeoRocketClient client = createClient();
    client.getTags().removeTags(query, layer, tags, ar -> {
      if (ar.failed()) {
        client.close();
        Throwable t = ar.cause();
        error(t.getMessage());
        if (!(t instanceof NoStackTraceThrowable)) {
          log.error("Could not remove the tags", t);
        }
        handler.handle(1);
      } else {
        handler.handle(0);
      }
    });
  }
}
