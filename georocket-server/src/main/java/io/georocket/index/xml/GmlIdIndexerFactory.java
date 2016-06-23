package io.georocket.index.xml;

import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.google.common.collect.ImmutableMap;

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
        "type", "string", // array of strings actually, auto-supported by Elasticsearch
        "index", "not_analyzed" // do not analyze (i.e. tokenize) this field, use the actual value
    )));
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    return MatchPriority.SHOULD;
  }

  @Override
  public QueryBuilder compileQuery(String search) {
    return QueryBuilders.termQuery("gmlIds", search);
  }
}
