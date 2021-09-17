package io.georocket.index.generic;

import com.google.common.collect.ImmutableMap;
import io.georocket.index.IndexerFactory;
import io.georocket.query.ElasticsearchQueryHelper;
import io.georocket.query.KeyValueQueryPart;
import io.georocket.query.KeyValueQueryPart.ComparisonOperator;
import io.georocket.query.QueryPart;
import io.georocket.query.StringQueryPart;
import io.vertx.core.json.JsonObject;

/**
 * Base class for factories creating indexers that manage arbitrary generic
 * string attributes (i.e. key-value pairs)
 * @author Michel Kraemer
 */
public abstract class GenericAttributeIndexerFactory implements IndexerFactory {
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
    if (queryPart instanceof StringQueryPart) {
      // match values of all fields regardless of their name
      String search = ((StringQueryPart)queryPart).getSearchString();
      return ElasticsearchQueryHelper.multiMatchQuery(search, "genAttrs.*");
    } else if (queryPart instanceof KeyValueQueryPart) {
      KeyValueQueryPart kvqp = (KeyValueQueryPart)queryPart;
      String key = kvqp.getKey();
      String value = kvqp.getValue();
      ComparisonOperator comp = kvqp.getComparisonOperator();

      switch (comp) {
        case EQ:
          return new JsonObject(ImmutableMap.of("genAttrs." + key, value));
        case GT:
          return new JsonObject(ImmutableMap.of("genAttrs." + key, ImmutableMap.of("$gt", value)));
        case GTE:
          return new JsonObject(ImmutableMap.of("genAttrs." + key, ImmutableMap.of("$gte", value)));
        case LT:
          return new JsonObject(ImmutableMap.of("genAttrs." + key, ImmutableMap.of("$lt", value)));
        case LTE:
          return new JsonObject(ImmutableMap.of("genAttrs." + key, ImmutableMap.of("$lte", value)));
      }
    }
    return null;
  }
}
