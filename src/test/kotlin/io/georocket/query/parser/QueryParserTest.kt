package io.georocket.query.parser

import io.georocket.query.parser.QueryParser.AndContext
import io.georocket.query.parser.QueryParser.EqContext
import io.georocket.query.parser.QueryParser.GtContext
import io.georocket.query.parser.QueryParser.GteContext
import io.georocket.query.parser.QueryParser.KeyvalueContext
import io.georocket.query.parser.QueryParser.LtContext
import io.georocket.query.parser.QueryParser.LteContext
import io.georocket.query.parser.QueryParser.NotContext
import io.georocket.query.parser.QueryParser.NumberContext
import io.georocket.query.parser.QueryParser.OrContext
import io.georocket.query.parser.QueryParser.StringContext
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.antlr.v4.runtime.ANTLRInputStream
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.Test

/**
 * Test [QueryParser]
 * @author Michel Kraemer
 */
class QueryParserTest {
  /**
   * Convert a parse tree to a JsonObject
   */
  private class ToJsonTreeListener : QueryBaseListener() {
    companion object {
      const val TYPE = "type"
      const val TEXT = "text"
      const val QUERY = "query"
      const val NUMBER = "number"
      const val STRING = "string"
      const val OR = "or"
      const val AND = "and"
      const val NOT = "not"
      const val EQ = "eq"
      const val LT = "lt"
      const val LTE = "lte"
      const val GT = "gt"
      const val GTE = "gte"
      const val KEYVALUE = "keyvalue"
      const val CHILDREN = "children"
    }

    var tree = ArrayDeque<JsonObject>()

    init {
      push(QUERY)
    }

    private fun push(type: String, text: String? = null) {
      val obj = JsonObject().put(TYPE, type)
      if (text != null) {
        obj.put(TEXT, text)
      }
      if (!tree.isEmpty()) {
        var children = tree.first().getJsonArray(CHILDREN)
        if (children == null) {
          children = JsonArray()
          tree.first().put(CHILDREN, children)
        }
        children.add(obj)
      }
      tree.addFirst(obj)
    }

    override fun enterOr(ctx: OrContext) {
      push(OR)
    }

    override fun exitOr(ctx: OrContext) {
      tree.removeFirst()
    }

    override fun enterAnd(ctx: AndContext) {
      push(AND)
    }

    override fun exitAnd(ctx: AndContext) {
      tree.removeFirst()
    }

    override fun enterNot(ctx: NotContext) {
      push(NOT)
    }

    override fun exitNot(ctx: NotContext) {
      tree.removeFirst()
    }

    override fun enterEq(ctx: EqContext) {
      push(EQ)
    }

    override fun exitEq(ctx: EqContext) {
      tree.removeFirst()
    }

    override fun enterGt(ctx: GtContext) {
      push(GT)
    }

    override fun exitGt(ctx: GtContext) {
      tree.removeFirst()
    }

    override fun enterGte(ctx: GteContext) {
      push(GTE)
    }

    override fun exitGte(ctx: GteContext) {
      tree.removeFirst()
    }

    override fun enterLt(ctx: LtContext) {
      push(LT)
    }

    override fun exitLt(ctx: LtContext) {
      tree.removeFirst()
    }

    override fun enterLte(ctx: LteContext) {
      push(LTE)
    }

    override fun exitLte(ctx: LteContext) {
      tree.removeFirst()
    }

    override fun enterKeyvalue(ctx: KeyvalueContext) {
      push(KEYVALUE)
    }

    override fun exitKeyvalue(ctx: KeyvalueContext) {
      tree.removeFirst()
    }

    override fun enterNumber(ctx: NumberContext) {
      push(NUMBER, ctx.text)
    }

    override fun exitNumber(ctx: NumberContext) {
      tree.removeFirst()
    }

    override fun enterString(ctx: StringContext) {
      push(STRING, ctx.text)
    }

    override fun exitString(ctx: StringContext) {
      tree.removeFirst()
    }

    override fun visitErrorNode(node: ErrorNode) {
      fail<Unit>("Hit error node")
    }
  }

  /**
   * Load a [fixture], parse and check the result
   */
  private fun expectFixture(fixture: String) {
    // load file
    val fixtureStr = this.javaClass.getResource("fixtures/$fixture.json")!!.readText()

    // get query and expected tree
    val fixtureObj = JsonObject(fixtureStr)
    val query = fixtureObj.getString("query")
    val expected = fixtureObj.getJsonObject("expected")

    // parse query
    val lexer = QueryLexer(ANTLRInputStream(query.trim()))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val ctx = parser.query()
    val listener = ToJsonTreeListener()
    ParseTreeWalker.DEFAULT.walk(listener, ctx)

    // assert tree
    assertThat(listener.tree).hasSize(1)
    val root = listener.tree.removeFirst()
    assertThat(root).isEqualTo(expected)
  }

  /**
   * Query with a single string
   */
  @Test
  fun string() {
    expectFixture("string")
  }

  /**
   * Query with two strings
   */
  @Test
  fun strings() {
    expectFixture("strings")
  }

  /**
   * EQuals
   */
  @Test
  fun eq() {
    expectFixture("eq")
  }

  /**
   * Greater than (GT)
   */
  @Test
  fun gt() {
    expectFixture("gt")
  }

  /**
   * Greater than or equal (GTE)
   */
  @Test
  fun gte() {
    expectFixture("gte")
  }

  /**
   * Less than (LT)
   */
  @Test
  fun lt() {
    expectFixture("lt")
  }

  /**
   * Less than or equal (LTE)
   */
  @Test
  fun lte() {
    expectFixture("lte")
  }

  /**
   * Explicit OR
   */
  @Test
  fun or() {
    expectFixture("or")
  }

  /**
   * Logical AND
   */
  @Test
  fun and() {
    expectFixture("and")
  }

  /**
   * Logical NOT
   */
  @Test
  operator fun not() {
    expectFixture("not")
  }

  /**
   * Logical NOT with nested EQ
   */
  @Test
  fun notEq() {
    expectFixture("not_eq")
  }

  /**
   * Query with a double-quoted string
   */
  @Test
  fun doubleQuotedString() {
    expectFixture("double_quoted_string")
  }

  /**
   * Query with a single-quoted string
   */
  @Test
  fun singleQuotedString() {
    expectFixture("single_quoted_string")
  }

  /**
   * Strings with digit in them
   */
  @Test
  fun stringsWithDigits() {
    expectFixture("strings_with_digits")
  }

  /**
   * Query with a quoted OR
   */
  @Test
  fun quotedOr() {
    expectFixture("quoted_or")
  }

  /**
   * Integer number
   */
  @Test
  fun integer() {
    expectFixture("integer")
  }

  /**
   * Various numbers
   */
  @Test
  fun numbers() {
    expectFixture("numbers")
  }
}
