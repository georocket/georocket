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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import io.georocket.query.parser.QueryBaseListener;
import io.georocket.query.parser.QueryLexer;
import io.georocket.query.parser.QueryParser;
import io.georocket.query.parser.QueryParser.AndContext;
import io.georocket.query.parser.QueryParser.EqContext;
import io.georocket.query.parser.QueryParser.GtContext;
import io.georocket.query.parser.QueryParser.KeyvalueContext;
import io.georocket.query.parser.QueryParser.LtContext;
import io.georocket.query.parser.QueryParser.NotContext;
import io.georocket.query.parser.QueryParser.OrContext;
import io.georocket.query.parser.QueryParser.QueryContext;
import io.georocket.query.parser.QueryParser.StringContext;
import io.georocket.util.PathUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Default implementation of {@link QueryCompiler}
 * @author Michel Kraemer
 */
public class DefaultQueryCompiler implements QueryCompiler {
  /**
   * Query compilers for individual properties
   */
  protected Collection<? extends QueryCompiler> queryCompilers;

  /**
   * Default constructor
   */
  public DefaultQueryCompiler() {
    this.queryCompilers = Collections.emptyList();
  }

  /**
   * Constructs the compiler
   * @param queryCompilers query compilers for individual properties
   */
  public void setQueryCompilers(Collection<? extends QueryCompiler> queryCompilers) {
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
   * Handle a query part. Pass it to all query compilers and return
   * an Elasticsearch query
   * @param str a string part of a query
   * @return the Elasticsearch query
   */
  protected JsonObject makeQuery(QueryPart str) {
    JsonObject bqb = boolQuery();
    for (QueryCompiler f : queryCompilers) {
      MatchPriority mp = f.getQueryPriority(str);
      if (mp == null) {
        continue;
      }
      
      // combine queries
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
    
    // optimize query - return single query embedded inside a
    // boolean expression
    if (bqb.size() == 1) {
      JsonObject bool = bqb.getJsonObject("bool");
      if (bool != null && bool.size() == 1) {
        String fieldName = bool.fieldNames().iterator().next();
        Object expr = bool.getValue(fieldName);
        if (expr instanceof JsonObject) {
          bqb = (JsonObject)expr;
        } else if (expr instanceof JsonArray) {
          JsonArray arrexpr = (JsonArray)expr;
          if (arrexpr.size() == 1) {
            bqb = arrexpr.getJsonObject(0);
          }
        }
      }
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
    
    /**
     * An object holding the current key-value pair and its comparator
     * (null if we are not parsing a key-value pair at the moment)
     */
    JsonObject currentKeyvalue = null;
    
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
      String str = ctx.getText();
      if (currentKeyvalue != null) {
        if (currentKeyvalue.containsKey("key")) {
          currentKeyvalue.put("value", str);
        } else {
          currentKeyvalue.put("key", str);
        }
      } else {
        StringQueryPart sqp = new StringQueryPart(str);
        JsonObject q = makeQuery(sqp);
        if (!combine(q)) {
          result.push(q);
        }
      }
    }

    @Override
    public void enterLt(LtContext ctx) {
      currentKeyvalue = new JsonObject();
      currentKeyvalue.put("comp", Comparator.LT.name());
    }

    @Override
    public void enterGt(GtContext ctx) {
      currentKeyvalue = new JsonObject();
      currentKeyvalue.put("comp", Comparator.GT.name());
    }

    @Override
    public void enterEq(EqContext ctx) {
      currentKeyvalue = new JsonObject();
      currentKeyvalue.put("comp", Comparator.EQ.name());
    }

    @Override
    public void exitKeyvalue(KeyvalueContext ctx) {
      KeyValueQueryPart kvqp = new KeyValueQueryPart(
        currentKeyvalue.getString("key"),
        currentKeyvalue.getString("value"),
        Comparator.valueOf(currentKeyvalue.getString("comp")));
      JsonObject q = makeQuery(kvqp);
      if (!combine(q)) {
        result.push(q);
      }
      currentKeyvalue = null;
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
