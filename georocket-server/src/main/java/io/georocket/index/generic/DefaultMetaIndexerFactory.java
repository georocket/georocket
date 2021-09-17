package io.georocket.index.generic;

import com.google.common.collect.ImmutableMap;
import io.georocket.index.xml.MetaIndexer;
import io.georocket.index.xml.MetaIndexerFactory;
import io.georocket.query.ElasticsearchQueryHelper;
import io.georocket.query.KeyValueQueryPart;
import io.georocket.query.KeyValueQueryPart.ComparisonOperator;
import io.georocket.query.QueryPart;
import io.georocket.query.StringQueryPart;
import io.vertx.core.json.JsonObject;

/**
 * Factory for {@link DefaultMetaIndexer} instances. Contains default mappings
 * required for essential GeoRocket indexer operations.
 * @author Michel Kraemer
 */
public class DefaultMetaIndexerFactory implements MetaIndexerFactory {
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
      result = new JsonObject(ImmutableMap.of("tags", search));
    } else if (queryPart instanceof KeyValueQueryPart) {
      KeyValueQueryPart kvqp = (KeyValueQueryPart)queryPart;
      String key = kvqp.getKey();
      String value = kvqp.getValue();
      ComparisonOperator comp = kvqp.getComparisonOperator();

      switch (comp) {
        case EQ:
          result = new JsonObject(ImmutableMap.of("props." + key, value));
          break;
        case GT:
          result = new JsonObject(ImmutableMap.of("props." + key, ImmutableMap.of("$gt", value)));
          break;
        case GTE:
          result = new JsonObject(ImmutableMap.of("props." + key, ImmutableMap.of("$gte", value)));
          break;
        case LT:
          result = new JsonObject(ImmutableMap.of("props." + key, ImmutableMap.of("$lt", value)));
          break;
        case LTE:
          result = new JsonObject(ImmutableMap.of("props." + key, ImmutableMap.of("$lte", value)));
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
