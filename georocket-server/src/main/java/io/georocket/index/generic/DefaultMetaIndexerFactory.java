package io.georocket.index.generic;

import static io.georocket.query.ElasticsearchQueryHelper.termQuery;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableMap;

import io.georocket.index.xml.MetaIndexer;
import io.georocket.index.xml.MetaIndexerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Factory for {@link DefaultMetaIndexer} instances. Contains default mappings
 * required for essential GeoRocket indexer operations.
 * @author Michel Kraemer
 */
public class DefaultMetaIndexerFactory implements MetaIndexerFactory {
  private final Map<String, Object> mappings;

  /**
   * Default constructor
   */
  @SuppressWarnings("unchecked")
  public DefaultMetaIndexerFactory() {
    // load default mapping
    Yaml yaml = new Yaml();
    Map<String, Object> mappings;
    try (InputStream is = this.getClass().getResourceAsStream("index_defaults.yaml")) {
      mappings = (Map<String, Object>)yaml.load(is);
    } catch (IOException e) {
      throw new RuntimeException("Could not load default mappings", e);
    }

    // remove unnecessary node
    mappings.remove("variables");

    this.mappings = ImmutableMap.copyOf(mappings);
  }

  @Override
  public Map<String, Object> getMapping() {
    return mappings;
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    return MatchPriority.SHOULD;
  }

  @Override
  public JsonObject compileQuery(String search) {
    return termQuery("tags", search);
  }

  @Override
  public MetaIndexer createIndexer() {
    return new DefaultMetaIndexer();
  }
}
