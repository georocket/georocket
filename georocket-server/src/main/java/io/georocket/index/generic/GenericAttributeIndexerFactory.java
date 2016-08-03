package io.georocket.index.generic;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.georocket.index.IndexerFactory;
import io.georocket.query.ElasticsearchQueryHelper;
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
                "index", "not_analyzed"
            )
        )
    )), "properties", ImmutableMap.of("genAttrs", ImmutableMap.of(
        // TODO remove hidden field when issue 16098 has been fixed
        // https://github.com/elastic/elasticsearch/issues/16098
        "properties", ImmutableMap.of(
            "$$$$HIDDEN_FIELD_WORKAROUND_FOR_ELASTICSEARCH_ISSUE_16098$$$$", ImmutableMap.of(
                "type", "string",
                "index", "no"
            )
        )
    )));
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    return MatchPriority.SHOULD;
  }

  @Override
  public JsonObject compileQuery(String search) {
    // match values of all fields regardless of their name
    return ElasticsearchQueryHelper.multiMatchQuery(search, "genAttrs.*");
  }
}
