package io.georocket.index.generic;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableMap;

import io.georocket.index.xml.MetaIndexer;
import io.georocket.index.xml.MetaIndexerFactory;
import io.georocket.query.ElasticsearchQueryHelper;
import io.georocket.query.KeyValueQueryPart;
import io.georocket.query.QueryPart;
import io.georocket.query.StringQueryPart;
import io.georocket.query.KeyValueQueryPart.ComparisonOperator;
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
  public MatchPriority getQueryPriority(QueryPart queryPart) {
    if (queryPart instanceof StringQueryPart ||
            queryPart instanceof KeyValueQueryPart) {
      return MatchPriority.SHOULD;
    }
    return MatchPriority.NONE;
  }

  @Override
  public JsonObject compileQuery(QueryPart queryPart) {
    JsonObject result = null;
    if (queryPart instanceof StringQueryPart) {
      // match values of all fields regardless of their name
      String search = ((StringQueryPart)queryPart).getSearchString();
      result = ElasticsearchQueryHelper.termQuery("tags", search);
    } else if (queryPart instanceof KeyValueQueryPart) {
      KeyValueQueryPart kvqp = (KeyValueQueryPart)queryPart;
      String key = kvqp.getKey();
      String value = kvqp.getValue();
      ComparisonOperator comp = kvqp.getComparisonOperator();

      switch (comp) {
        case EQ:
          result = ElasticsearchQueryHelper.termQuery("props." + key, value);
          break;
        case GT:
          result = ElasticsearchQueryHelper.gtQuery("props." + key, value);
          break;
        case GTE:
          result = ElasticsearchQueryHelper.gteQuery("props." + key, value);
          break;
        case LT:
          result = ElasticsearchQueryHelper.ltQuery("props." + key, value);
          break;
        case LTE:
          result = ElasticsearchQueryHelper.lteQuery("props." + key, value);
          break;
      }

      if (kvqp.getComparisonOperator() == ComparisonOperator.EQ &&
          "correlationId".equals(kvqp.getKey())) {
        JsonObject cq = ElasticsearchQueryHelper.termQuery("correlationId", value);
        if (result != null) {
          JsonObject bool = ElasticsearchQueryHelper.boolQuery(1);
          ElasticsearchQueryHelper.boolAddShould(bool, result);
          ElasticsearchQueryHelper.boolAddShould(bool, cq);
          result = bool;
        } else {
          result = cq;
        }
      }
    }
    return result;
  }

  @Override
  public MetaIndexer createIndexer() {
    return new DefaultMetaIndexer();
  }
}
