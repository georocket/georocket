package io.georocket.query;

import io.georocket.util.PathUtils;
import io.vertx.core.json.JsonObject;

import java.util.Collection;

import static io.georocket.query.ElasticsearchQueryHelper.boolAddMust;
import static io.georocket.query.ElasticsearchQueryHelper.boolAddShould;
import static io.georocket.query.ElasticsearchQueryHelper.boolQuery;
import static io.georocket.query.ElasticsearchQueryHelper.prefixQuery;
import static io.georocket.query.ElasticsearchQueryHelper.termQuery;

/**
 * Support chunk-related queries.
 * @author Benedikt Hiemenz
 */
public class ChunkQueryCompiler extends DefaultQueryCompiler {

  public ChunkQueryCompiler(Collection<? extends QueryCompiler> queryCompilers) {
    super(queryCompilers);
  }

  @Override
  public JsonObject compileQuery(String search, String path) {
    JsonObject qb = compileQuery(search);
    if (path != null && !path.equals("/")) {
      String prefix = PathUtils.addTrailingSlash(path);

      JsonObject qi = boolQuery();
      boolAddShould(qi, termQuery("_id", path));
      boolAddShould(qi, prefixQuery("_id", prefix));

      JsonObject qr = boolQuery();
      boolAddShould(qr, qb);
      boolAddMust(qr, qi);

      return qr;
    }
    return qb;
  }

  @Override
  protected JsonObject makeStringQuery(String str) {
    // general query
    JsonObject tagsQuery = termQuery("tags", str);

    // pass on string part to other query compilers
    JsonObject bqb = null;
    for (QueryCompiler f : queryCompilers) {
      MatchPriority mp = f.getQueryPriority(str);
      if (mp == null) {
        continue;
      }

      // combine queries
      if (bqb == null && mp != MatchPriority.NONE) {
        bqb = boolQuery();
        boolAddShould(bqb, tagsQuery);
      }
      switch (mp) {
        case ONLY:
          return f.compileQuery(str);
        case SHOULD:
          boolAddShould(bqb, f.compileQuery(str));
          break;
        case MUST:
          boolAddMust(bqb, f.compileQuery(str));
          break;
        case NONE:
          break;
      }
    }

    if (bqb == null) {
      return tagsQuery;
    }
    return bqb;
  }
}
