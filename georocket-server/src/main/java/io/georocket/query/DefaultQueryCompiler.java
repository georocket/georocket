package io.georocket.query;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import io.georocket.api.index.xml.XMLIndexerFactory;
import io.georocket.api.query.QueryCompiler;
import io.georocket.util.PathUtils;
import io.georocket.util.QuotedStringSplitter;

/**
 * Default implementation of {@link QueryCompiler}
 * @author Michel Kraemer
 */
public class DefaultQueryCompiler implements QueryCompiler {
  /**
   * Query compilers for individual properties
   */
  private final Iterable<? extends QueryCompiler> queryCompilers;
  
  /**
   * Default constructor
   */
  public DefaultQueryCompiler() {
    this(ServiceLoader.load(XMLIndexerFactory.class));
  }
  
  /**
   * Constructs the compiler
   * @param queryCompilers query compilers for individual properties
   */
  public DefaultQueryCompiler(Iterable<? extends QueryCompiler> queryCompilers) {
    if (queryCompilers == null) {
      this.queryCompilers = Collections.emptyList();
    } else {
      this.queryCompilers = queryCompilers;
    }
  }
  
  /**
   * Compile a search string
   * @param search the search string to compile
   * @param path the path where to perform the search (may be null if the
   * whole data store should be searched)
   * @return the compiled query
   */
  public QueryBuilder compileQuery(String search, String path) {
    QueryBuilder qb = compileQuery(search);
    if (path != null && !path.equals("/")) {
      String prefix = PathUtils.addTrailingSlash(path);
      return QueryBuilders.boolQuery()
          .should(qb)
          .must(QueryBuilders.boolQuery()
              .should(QueryBuilders.termQuery("_id", path))
              .should(QueryBuilders.prefixQuery("_id", prefix)));
    }
    return qb;
  }
  
  @Override
  public QueryBuilder compileQuery(String search) {
    if (search == null || search.isEmpty()) {
      // match everything my default
      return QueryBuilders.matchAllQuery();
    }
    
    // split search query
    List<String> searches = QuotedStringSplitter.split(search);
    if (searches.size() == 1) {
      search = searches.get(0);
      
      TermQueryBuilder tagsQuery = QueryBuilders.termQuery("tags", search);
      
      BoolQueryBuilder bqb = null;
      for (QueryCompiler f : queryCompilers) {
        MatchPriority mp = f.getQueryPriority(search);
        if (mp == null) {
          continue;
        }
        
        if (bqb == null && mp != MatchPriority.NONE) {
          bqb = QueryBuilders.boolQuery();
          bqb.should(tagsQuery);
        }
        
        switch (mp) {
        case ONLY:
          return f.compileQuery(search);
        case SHOULD:
          bqb.should(f.compileQuery(search));
          break;
        case MUST:
          bqb.must(f.compileQuery(search));
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
    
    // call #makeQuery for every part of the search query recursively
    BoolQueryBuilder bqb = QueryBuilders.boolQuery();
    searches.stream().map(this::compileQuery).forEach(bqb::should);
    return bqb;
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    return MatchPriority.MUST;
  }
}
