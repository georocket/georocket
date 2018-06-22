package io.georocket.index.xml;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.georocket.query.ElasticsearchQueryHelper;
import io.georocket.query.KeyValueQueryPart;
import io.georocket.query.KeyValueQueryPart.ComparisonOperator;
import io.georocket.query.QueryPart;
import io.georocket.query.StringQueryPart;
import io.vertx.core.json.JsonObject;

/**
 * Create instances of {@link GmlIdIndexer}
 * @author Michel Kraemer
 */
public class GmlIdIndexerFactory implements XMLIndexerFactory {
  @Override
  public XMLIndexer createIndexer() {
    return new GmlIdIndexer();
  }
  
  @Override
  public Map<String, Object> getMapping() {
    return ImmutableMap.of("properties", ImmutableMap.of("gmlIds", ImmutableMap.of(
        "type", "keyword" // array of keywords actually, auto-supported by Elasticsearch
    )));
  }

  /**
   * Test if the given key-value query part refers to a gmlId and if it uses
   * the EQ operator (e.g. EQ(gmlId myId) or EQ(gml:id myId))
   * @param kvqp the key-value query part to check
   * @return true if it refers to a gmlId, false otherwise
   */
  private static boolean isGmlIdEQ(KeyValueQueryPart kvqp) {
    String key = kvqp.getKey();
    ComparisonOperator comp = kvqp.getComparisonOperator();
    return (comp == ComparisonOperator.EQ && ("gmlId".equals(key) || "gml:id".equals(key)));
  }

  @Override
  public MatchPriority getQueryPriority(QueryPart queryPart) {
    if (queryPart instanceof StringQueryPart) {
      return MatchPriority.SHOULD;
    }
    if (queryPart instanceof KeyValueQueryPart && isGmlIdEQ((KeyValueQueryPart)queryPart)) {
      return MatchPriority.SHOULD;
    }
    return MatchPriority.NONE;
  }

  @Override
  public JsonObject compileQuery(QueryPart queryPart) {
    if (queryPart instanceof StringQueryPart) {
      String search = ((StringQueryPart)queryPart).getSearchString();
      return ElasticsearchQueryHelper.termQuery("gmlIds", search);
    } else if (queryPart instanceof KeyValueQueryPart) {
      KeyValueQueryPart kvqp = (KeyValueQueryPart)queryPart;
      if (isGmlIdEQ(kvqp)) {
        return ElasticsearchQueryHelper.termQuery("gmlIds", kvqp.getValue());
      }
    }
    return null;
  }
}
