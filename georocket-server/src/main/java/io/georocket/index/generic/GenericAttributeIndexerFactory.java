package io.georocket.index.generic;

import java.util.Map;

import com.google.common.collect.ImmutableList;
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
  public Map<String, Object> getMapping() {
    // dynamic mapping: do not analyze generic attributes
    return ImmutableMap.of("dynamic_templates", ImmutableList.of(ImmutableMap.of(
        "genAttrsFields", ImmutableMap.of(
            "path_match", "genAttrs.*",
            "mapping", ImmutableMap.of(
                "type", "keyword"
            )
        )
    )));
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
          return ElasticsearchQueryHelper.termQuery("genAttrs." + key, value);
        case GT:
          return ElasticsearchQueryHelper.gtQuery("genAttrs." + key, value);
        case GTE:
          return ElasticsearchQueryHelper.gteQuery("genAttrs." + key, value);
        case LT:
          return ElasticsearchQueryHelper.ltQuery("genAttrs." + key, value);
        case LTE:
          return ElasticsearchQueryHelper.lteQuery("genAttrs." + key, value);
      }
    }
    return null;
  }
}
