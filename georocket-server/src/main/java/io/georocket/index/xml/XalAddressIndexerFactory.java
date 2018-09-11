package io.georocket.index.xml;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.georocket.query.KeyValueQueryPart;
import io.georocket.query.KeyValueQueryPart.ComparisonOperator;
import io.georocket.query.QueryPart;
import io.georocket.query.StringQueryPart;
import io.vertx.core.json.JsonObject;

import java.util.Map;

import static io.georocket.query.ElasticsearchQueryHelper.gtQuery;
import static io.georocket.query.ElasticsearchQueryHelper.gteQuery;
import static io.georocket.query.ElasticsearchQueryHelper.ltQuery;
import static io.georocket.query.ElasticsearchQueryHelper.lteQuery;
import static io.georocket.query.ElasticsearchQueryHelper.multiMatchQuery;
import static io.georocket.query.ElasticsearchQueryHelper.termQuery;

/**
 * Creates instances of {@link XalAddressIndexer}
 * @author Michel Kraemer
 */
public class XalAddressIndexerFactory implements XMLIndexerFactory {
  @Override
  public XMLIndexer createIndexer() {
    return new XalAddressIndexer();
  }

  @Override
  public Map<String, Object> getMapping() {
    return ImmutableMap.of("dynamic_templates", ImmutableList.of(ImmutableMap.of(
      "addressFields", ImmutableMap.of(
        "path_match", "address.*",
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
      return multiMatchQuery(search, "address.*");
    } else if (queryPart instanceof KeyValueQueryPart) {
      KeyValueQueryPart kvqp = (KeyValueQueryPart)queryPart;
      String key = kvqp.getKey();
      String value = kvqp.getValue();
      ComparisonOperator comp = kvqp.getComparisonOperator();
      String name = "address." + key;
      switch (comp) {
        case EQ:
          return termQuery(name, value);
        case GT:
          return gtQuery(name, value);
        case GTE:
          return gteQuery(name, value);
        case LT:
          return ltQuery(name, value);
        case LTE:
          return lteQuery(name, value);
      }
    }
    return null;
  }
}
