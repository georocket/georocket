package io.georocket.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import io.georocket.query.parser.QueryParser.AndContext;
import io.georocket.query.parser.QueryParser.EqContext;
import io.georocket.query.parser.QueryParser.GtContext;
import io.georocket.query.parser.QueryParser.GteContext;
import io.georocket.query.parser.QueryParser.KeyvalueContext;
import io.georocket.query.parser.QueryParser.LtContext;
import io.georocket.query.parser.QueryParser.LteContext;
import io.georocket.query.parser.QueryParser.NotContext;
import io.georocket.query.parser.QueryParser.OrContext;
import io.georocket.query.parser.QueryParser.QueryContext;
import io.georocket.query.parser.QueryParser.StringContext;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Test {@link QueryParser}
 * @author Michel Kraemer
 */
public class QueryParserTest {
  /**
   * Convert a parse tree to a JsonObject
   */
  private static class ToJsonTreeListener extends QueryBaseListener {
    Deque<JsonObject> tree = new ArrayDeque<JsonObject>();
    
    final static String TYPE = "type";
    final static String TEXT = "text";
    final static String QUERY = "query";
    final static String STRING = "string";
    final static String OR = "or";
    final static String AND = "and";
    final static String NOT = "not";
    final static String EQ = "eq";
    final static String LT = "lt";
    final static String LTE = "lte";
    final static String GT = "gt";
    final static String GTE = "gte";
    final static String KEYVALUE = "keyvalue";
    final static String CHILDREN = "children";
    
    ToJsonTreeListener() {
      push(QUERY);
    }
    
    private void push(String type) {
      push(type, null);
    }
    
    private void push(String type, String text) {
      JsonObject obj = new JsonObject().put(TYPE, type);
      if (text != null) {
        obj.put(TEXT, text);
      }
      if (!tree.isEmpty()) {
        JsonArray children = tree.peek().getJsonArray(CHILDREN);
        if (children == null) {
          children = new JsonArray();
          tree.peek().put(CHILDREN, children);
        }
        children.add(obj);
      }
      tree.push(obj);
    }
    
    @Override
    public void enterOr(OrContext ctx) {
      push(OR);
    }
    
    @Override
    public void exitOr(OrContext ctx) {
      tree.pop();
    }
    
    @Override
    public void enterAnd(AndContext ctx) {
      push(AND);
    }
    
    @Override
    public void exitAnd(AndContext ctx) {
      tree.pop();
    }
    
    @Override
    public void enterNot(NotContext ctx) {
      push(NOT);
    }
    
    @Override
    public void exitNot(NotContext ctx) {
      tree.pop();
    }
    
    @Override
    public void enterEq(EqContext ctx) {
      push(EQ);
    }
    
    @Override
    public void exitEq(EqContext ctx) {
      tree.pop();
    }

    @Override
    public void enterGt(GtContext ctx) {
      push(GT);
    }

    @Override
    public void exitGt(GtContext ctx) {
      tree.pop();
    }

    @Override
    public void enterGte(GteContext ctx) {
      push(GTE);
    }

    @Override
    public void exitGte(GteContext ctx) {
      tree.pop();
    }

    @Override
    public void enterLt(LtContext ctx) {
      push(LT);
    }

    @Override
    public void exitLt(LtContext ctx) {
      tree.pop();
    }

    @Override
    public void enterLte(LteContext ctx) {
      push(LTE);
    }

    @Override
    public void exitLte(LteContext ctx) {
      tree.pop();
    }

    @Override
    public void enterKeyvalue(KeyvalueContext ctx) {
      push(KEYVALUE);
    }
    
    @Override
    public void exitKeyvalue(KeyvalueContext ctx) {
      tree.pop();
    }
    
    @Override
    public void enterString(StringContext ctx) {
      push(STRING, ctx.getText());
    }
    
    @Override
    public void exitString(StringContext ctx) {
      tree.pop();
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
      fail();
    }
  }
  
  /**
   * Load a fixture, parse and check the result
   * @param fixture the name of the fixture to load (without path and extension)
   */
  private void expectFixture(String fixture) {
    // load file
    URL u = this.getClass().getResource("fixtures/" + fixture + ".json");
    String fixtureStr;
    try {
      fixtureStr = IOUtils.toString(u, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    // get query and expected tree
    JsonObject fixtureObj = new JsonObject(fixtureStr);
    String query = fixtureObj.getString("query");
    JsonObject expected = fixtureObj.getJsonObject("expected");
    
    // parse query
    QueryLexer lexer = new QueryLexer(new ANTLRInputStream(query.trim()));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    QueryParser parser = new QueryParser(tokens);
    QueryContext ctx = parser.query();
    ToJsonTreeListener listener = new ToJsonTreeListener();
    ParseTreeWalker.DEFAULT.walk(listener, ctx);
    
    // assert tree
    assertEquals(1, listener.tree.size());
    JsonObject root = listener.tree.pop();
    assertEquals(expected, root);
  }
  
  /**
   * Query with a single string
   */
  @Test
  public void string() {
    expectFixture("string");
  }
  
  /**
   * Query with two strings
   */
  @Test
  public void strings() {
    expectFixture("strings");
  }
  
  /**
   * EQuals
   */
  @Test
  public void eq() {
    expectFixture("eq");
  }

  /**
   * Greater than (GT)
   */
  @Test
  public void gt() {
    expectFixture("gt");
  }

  /**
   * Greater than or equal (GTE)
   */
  @Test
  public void gte() {
    expectFixture("gte");
  }

  /**
   * Less than (LT)
   */
  @Test
  public void lt() {
    expectFixture("lt");
  }

  /**
   * Less than or equal (LTE)
   */
  @Test
  public void lte() {
    expectFixture("lte");
  }

  /**
   * Explicit OR
   */
  @Test
  public void or() {
    expectFixture("or");
  }
  
  /**
   * Logical AND
   */
  @Test
  public void and() {
    expectFixture("and");
  }
  
  /**
   * Logical NOT
   */
  @Test
  public void not() {
    expectFixture("not");
  }

  /**
   * Logical NOT with nested EQ
   */
  @Test
  public void notEq() {
    expectFixture("not_eq");
  }

  /**
   * Query with a double-quoted string
   */
  @Test
  public void doubleQuotedString() {
    expectFixture("double_quoted_string");
  }
  
  /**
   * Query with a single-quoted string
   */
  @Test
  public void singleQuotedString() {
    expectFixture("single_quoted_string");
  }
  
  /**
   * Query with a quoted OR
   */
  @Test
  public void quotedOr() {
    expectFixture("quoted_or");
  }
}
