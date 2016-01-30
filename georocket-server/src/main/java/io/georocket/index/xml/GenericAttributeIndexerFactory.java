package io.georocket.index.xml;

import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.georocket.api.index.xml.XMLIndexer;
import io.georocket.api.index.xml.XMLIndexerFactory;

/**
 * Create instances of {@link GenericAttributeIndexer}
 * @author Michel Kraemer
 */
public class GenericAttributeIndexerFactory implements XMLIndexerFactory {
  @Override
  public XMLIndexer createIndexer() {
    return new GenericAttributeIndexer();
  }

  @Override
  public Map<String, Object> getMapping() {
    // dynamic mapping: do not analyze generic attributes
    return ImmutableMap.of("dynamic_templates", ImmutableList.of(ImmutableMap.of(
        "genAttrsFields", ImmutableMap.of(
            "path_match", "genAttrs.*",
            "mapping", ImmutableMap.of(
                "index", "not_analyzed"
            )
        )
    )));
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    return MatchPriority.SHOULD;
  }

  @Override
  public QueryBuilder compileQuery(String search) {
    // match values of all fields regardless of their name
    return QueryBuilders.multiMatchQuery(search, "genAttrs.*");
  }
}
