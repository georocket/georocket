package io.georocket.query;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.ServiceLoader;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import io.georocket.api.index.xml.XMLIndexerFactory;
import io.georocket.api.query.QueryCompiler;
import io.georocket.query.parser.QueryBaseListener;
import io.georocket.query.parser.QueryLexer;
import io.georocket.query.parser.QueryParser;
import io.georocket.query.parser.QueryParser.AndContext;
import io.georocket.query.parser.QueryParser.NotContext;
import io.georocket.query.parser.QueryParser.OrContext;
import io.georocket.query.parser.QueryParser.QueryContext;
import io.georocket.query.parser.QueryParser.StringContext;
import io.georocket.util.PathUtils;

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
      // match everything by default
      return QueryBuilders.matchAllQuery();
    }
    
    // parse query
    QueryLexer lexer = new QueryLexer(new ANTLRInputStream(search.trim()));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    QueryParser parser = new QueryParser(tokens);
    QueryContext ctx = parser.query();
    
    // compile query to QueryBuilder
    QueryCompilerListener listener = new QueryCompilerListener();
    ParseTreeWalker.DEFAULT.walk(listener, ctx);
    
    if (listener.result.isEmpty()) {
      return QueryBuilders.matchAllQuery();
    }
    return listener.result.pop();
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    return MatchPriority.MUST;
  }
  
  /**
   * Handle a string part of a query. Pass it to all query compilers and return
   * a suitable QueryBuilder instance
   * @param str a string part of a query
   * @return a QueryBuilder
   */
  private QueryBuilder makeStringQuery(String str) {
    // general query
    TermQueryBuilder tagsQuery = QueryBuilders.termQuery("tags", str);
    
    // pass on string part to other query compilers
    BoolQueryBuilder bqb = null;
    for (QueryCompiler f : queryCompilers) {
      MatchPriority mp = f.getQueryPriority(str);
      if (mp == null) {
        continue;
      }
      
      // combine queries
      if (bqb == null && mp != MatchPriority.NONE) {
        bqb = QueryBuilders.boolQuery();
        bqb.should(tagsQuery);
      }
      switch (mp) {
      case ONLY:
        return f.compileQuery(str);
      case SHOULD:
        bqb.should(f.compileQuery(str));
        break;
      case MUST:
        bqb.must(f.compileQuery(str));
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
  
  /**
   * Marker for the current logical operation
   */
  private static enum Logical {
    OR, AND, NOT
  }
  
  /**
   * A tree listener that compiles a QueryBuilder
   */
  private class QueryCompilerListener extends QueryBaseListener {
    /**
     * A stack holding the current logical operation on top
     */
    Deque<Logical> currentLogical = new ArrayDeque<>();
    
    /**
     * A stack holding the result QueryBuilder
     */
    Deque<QueryBuilder> result = new ArrayDeque<>();
    
    QueryCompilerListener() {
      // at root level all terms a combined by logical OR
      currentLogical.push(Logical.OR);
    }
    
    /**
     * Enter a logical expression
     * @param l the logical operation
     */
    private void enterLogical(Logical l) {
      BoolQueryBuilder bqb = QueryBuilders.boolQuery();
      combine(bqb);
      result.push(bqb);
      currentLogical.push(l);
    }
    
    /**
     * Exit a logical expression
     */
    private void exitLogical() {
      currentLogical.pop();
      if (result.size() > 1) {
        result.pop();
      }
    }
    
    @Override
    public void enterOr(OrContext ctx) {
      enterLogical(Logical.OR);
    }
    
    @Override
    public void exitOr(OrContext ctx) {
      exitLogical();
    }
    
    @Override
    public void enterAnd(AndContext ctx) {
      enterLogical(Logical.AND);
    }
    
    @Override
    public void exitAnd(AndContext ctx) {
      exitLogical();
    }
    
    @Override
    public void enterNot(NotContext ctx) {
      enterLogical(Logical.NOT);
    }
    
    @Override
    public void exitNot(NotContext ctx) {
      exitLogical();
    }
    
    @Override
    public void enterString(StringContext ctx) {
      QueryBuilder stringQuery = makeStringQuery(ctx.getText());
      if (!combine(stringQuery)) {
        result.push(stringQuery);
      }
    }
    
    /**
     * Combine a QueryBuilder into the one currently on top of the stack
     * @param other the QueryBuilder to combine
     * @return true if the QueryBuilder was combined or false if the stack
     * was empty
     */
    private boolean combine(QueryBuilder other) {
      QueryBuilder b = result.peek();
      if (b == null) {
        return false;
      }
      
      if (b instanceof BoolQueryBuilder) {
        // combine into existing boolean query
        BoolQueryBuilder bqb = (BoolQueryBuilder)b;
        Logical l = currentLogical.peek();
        switch (l) {
        case OR:
          bqb.should(other);
          break;
        case AND:
          bqb.must(other);
          break;
        case NOT:
          bqb.mustNot(other);
          break;
        }
      } else {
        // create a new boolean query and replace top of the stack
        result.pop();
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        Logical l = currentLogical.peek();
        switch (l) {
        case OR:
          bqb.should(b);
          bqb.should(other);
          break;
        case AND:
          bqb.must(b);
          bqb.must(other);
          break;
        case NOT:
          bqb.mustNot(b);
          bqb.mustNot(other);
          break;
        }
        result.push(bqb);
      }
      
      return true;
    }
  }
}
