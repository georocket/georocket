package io.georocket.query;

import static io.georocket.query.ElasticsearchQueryHelper.boolAddMust;
import static io.georocket.query.ElasticsearchQueryHelper.boolAddMustNot;
import static io.georocket.query.ElasticsearchQueryHelper.boolAddShould;
import static io.georocket.query.ElasticsearchQueryHelper.boolQuery;
import static io.georocket.query.ElasticsearchQueryHelper.matchAllQuery;
import static io.georocket.query.ElasticsearchQueryHelper.prefixQuery;
import static io.georocket.query.ElasticsearchQueryHelper.termQuery;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.ServiceLoader;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.google.common.collect.ImmutableList;

import io.georocket.index.xml.XMLIndexerFactory;
import io.georocket.query.parser.QueryBaseListener;
import io.georocket.query.parser.QueryLexer;
import io.georocket.query.parser.QueryParser;
import io.georocket.query.parser.QueryParser.AndContext;
import io.georocket.query.parser.QueryParser.NotContext;
import io.georocket.query.parser.QueryParser.OrContext;
import io.georocket.query.parser.QueryParser.QueryContext;
import io.georocket.query.parser.QueryParser.StringContext;
import io.georocket.util.PathUtils;
import io.vertx.core.json.JsonObject;

/**
 * Default implementation of {@link QueryCompiler}
 * @author Michel Kraemer
 */
public class DefaultQueryCompiler implements QueryCompiler {
  /**
   * Query compilers for individual properties
   */
  protected final Collection<? extends QueryCompiler> queryCompilers;
  
  /**
   * Default constructor
   */
  public DefaultQueryCompiler() {
    // load factories now and not lazily to avoid concurrent modifications to
    // the service loader's internal cache
    this(ImmutableList.copyOf(ServiceLoader.load(XMLIndexerFactory.class)));
  }
  
  /**
   * Constructs the compiler
   * @param queryCompilers query compilers for individual properties
   */
  public DefaultQueryCompiler(Collection<? extends QueryCompiler> queryCompilers) {
    if (queryCompilers == null) {
      this.queryCompilers = Collections.emptyList();
    } else {
      this.queryCompilers = queryCompilers;
    }
  }

  @Override
  public JsonObject compileQuery(String search, String path) {
    JsonObject qb = compileQuery(search);
    if (path != null && !path.equals("/")) {
      String prefix = PathUtils.addTrailingSlash(path);
      
      JsonObject qi = boolQuery();
      boolAddShould(qi, termQuery("path", path));
      boolAddShould(qi, prefixQuery("path", prefix));
      
      JsonObject qr = boolQuery();
      boolAddShould(qr, qb);
      boolAddMust(qr, qi);
      
      return qr;
    }
    return qb;
  }

  @Override
  public JsonObject compileQuery(String search) {
    if (search == null || search.isEmpty()) {
      // match everything by default
      return matchAllQuery();
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
      return matchAllQuery();
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
  
  /**
   * Marker for the current logical operation
   */
  protected enum Logical {
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
    Deque<JsonObject> result = new ArrayDeque<>();
    
    QueryCompilerListener() {
      // at root level all terms a combined by logical OR
      currentLogical.push(Logical.OR);
    }
    
    /**
     * Enter a logical expression
     * @param l the logical operation
     */
    private void enterLogical(Logical l) {
      JsonObject bqb = boolQuery();
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
      JsonObject stringQuery = makeStringQuery(ctx.getText());
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
    private boolean combine(JsonObject other) {
      JsonObject b = result.peek();
      if (b == null) {
        return false;
      }
      
      if (b.containsKey("bool")) {
        // combine into existing boolean query
        Logical l = currentLogical.peek();
        switch (l) {
        case OR:
          boolAddShould(b, other);
          break;
        case AND:
          boolAddMust(b, other);
          break;
        case NOT:
          boolAddMustNot(b, other);
          break;
        }
      } else {
        // create a new boolean query and replace top of the stack
        result.pop();
        JsonObject bqb = boolQuery();
        Logical l = currentLogical.peek();
        switch (l) {
        case OR:
          boolAddShould(bqb, b);
          boolAddShould(bqb, other);
          break;
        case AND:
          boolAddMust(bqb, b);
          boolAddMust(bqb, other);
          break;
        case NOT:
          boolAddMustNot(bqb, b);
          boolAddMustNot(bqb, other);
          break;
        }
        result.push(bqb);
      }
      
      return true;
    }
  }
}
