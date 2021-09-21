package io.georocket.query

import io.georocket.query.KeyValueQueryPart.ComparisonOperator
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.parser.QueryBaseListener
import io.georocket.query.parser.QueryLexer
import io.georocket.query.parser.QueryParser
import io.georocket.query.parser.QueryParser.AndContext
import io.georocket.query.parser.QueryParser.EqContext
import io.georocket.query.parser.QueryParser.GtContext
import io.georocket.query.parser.QueryParser.GteContext
import io.georocket.query.parser.QueryParser.KeyvalueContext
import io.georocket.query.parser.QueryParser.LtContext
import io.georocket.query.parser.QueryParser.LteContext
import io.georocket.query.parser.QueryParser.NotContext
import io.georocket.query.parser.QueryParser.OrContext
import io.georocket.query.parser.QueryParser.StringContext
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import org.antlr.v4.runtime.ANTLRInputStream
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ParseTreeWalker

/**
 * Default implementation of [QueryCompiler]
 * @author Michel Kraemer
 */
class DefaultQueryCompiler(private val queryCompilers: Collection<QueryCompiler>) :
  QueryCompiler {
  /**
   * Compile a [search] string with an optional chunk [path]
   */
  fun compileQuery(search: String, path: String?): JsonObject {
    var qb = compileQuery(search)
    if (path != null && path != "/") {
      qb = json {
        obj(
          "\$and" to array(
            qb,
            obj(
              "path" to obj(
                "\$regex" to "^${path.escapeRegex()}"
              )
            )
          )
        )
      }
    }
    return qb
  }

  override fun compileQuery(search: String): JsonObject {
    if (search.isEmpty()) {
      // match everything by default
      return JsonObject()
    }

    // parse query
    val lexer = QueryLexer(ANTLRInputStream(search.trim()))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val ctx = parser.query()

    // compile query to QueryBuilder
    val listener = QueryCompilerListener()
    ParseTreeWalker.DEFAULT.walk(listener, ctx)

    val operands = listener.result.firstOrNull() ?: listOf(JsonObject())

    return if (operands.size == 1) {
      operands.first()
    } else {
      json {
        obj(
          "\$or" to operands
        )
      }
    }
  }

  override fun getQueryPriority(search: String): MatchPriority {
    return MatchPriority.ONLY
  }

  /**
   * Handle a [queryPart]. Pass it to all query compilers and return a
   * MongoDB query.
   */
  private fun makeQuery(queryPart: QueryPart): JsonObject {
    val operands = mutableListOf<JsonObject>()
    for (f in queryCompilers) {
      val mp = f.getQueryPriority(queryPart) ?: continue
      when (mp) {
        MatchPriority.ONLY -> return f.compileQuery(queryPart)
        MatchPriority.SHOULD -> operands.add(f.compileQuery(queryPart))
        MatchPriority.NONE -> { /* ignore operand */ }
      }
    }

    return if (operands.size > 1) {
      json {
        obj(
          "\$or" to array(operands)
        )
      }
    } else {
      operands.first()
    }
  }

  /**
   * Marker for the current logical operation
   */
  private enum class Logical {
    OR, AND, NOT
  }

  private data class CurrentKeyValue(val key: String? = null, val value: String? = null,
    val comp: ComparisonOperator? = null)

  /**
   * A tree listener that compiles a QueryBuilder
   */
  private inner class QueryCompilerListener : QueryBaseListener() {
    /**
     * A stack holding the current logical operation on top
     */
    val currentLogical = ArrayDeque<Logical>()

    /**
     * A stack holding the result QueryBuilder
     */
    val result = ArrayDeque<MutableList<JsonObject>>()

    /**
     * An object holding the current key-value pair and its comparator
     * (`null` if we are not parsing a key-value pair at the moment)
     */
    var currentKeyvalue: CurrentKeyValue? = null

    init {
      // at root level all terms a combined by logical OR
      currentLogical.add(Logical.OR)
      result.add(mutableListOf())
    }

    /**
     * Enter a logical expression
     * @param l the logical operation
     */
    private fun enterLogical(l: Logical) {
      result.addFirst(mutableListOf())
      currentLogical.addFirst(l)
    }

    /**
     * Exit a logical expression
     */
    private fun exitLogical() {
      val l = currentLogical.removeFirst()

      val (operator, operands) = when (l) {
        Logical.OR -> "\$or" to result.removeFirst()
        Logical.AND -> "\$and" to result.removeFirst()
        Logical.NOT -> {
          val co = result.removeFirst()
          "\$not" to if (co.size == 1) {
            co.first()
          } else {
            json {
              obj(
                "\$or" to co
              )
            }
          }
        }
      }

      result.first().add(json {
        obj(
          operator to operands
        )
      })
    }

    override fun enterOr(ctx: OrContext) {
      enterLogical(Logical.OR)
    }

    override fun exitOr(ctx: OrContext) {
      exitLogical()
    }

    override fun enterAnd(ctx: AndContext) {
      enterLogical(Logical.AND)
    }

    override fun exitAnd(ctx: AndContext) {
      exitLogical()
    }

    override fun enterNot(ctx: NotContext) {
      enterLogical(Logical.NOT)
    }

    override fun exitNot(ctx: NotContext) {
      exitLogical()
    }

    override fun enterString(ctx: StringContext) {
      val str = ctx.text
      if (currentKeyvalue != null) {
        currentKeyvalue = if (currentKeyvalue?.key != null) {
          currentKeyvalue?.copy(value = str)
        } else {
          currentKeyvalue?.copy(key = str)
        }
      } else {
        val sqp = StringQueryPart(str)
        val q = makeQuery(sqp)
        result.first().add(q)
      }
    }

    override fun enterLt(ctx: LtContext) {
      currentKeyvalue = CurrentKeyValue(comp = ComparisonOperator.LT)
    }

    override fun enterLte(ctx: LteContext) {
      currentKeyvalue = CurrentKeyValue(comp = ComparisonOperator.LTE)
    }

    override fun enterGt(ctx: GtContext) {
      currentKeyvalue = CurrentKeyValue(comp = ComparisonOperator.GT)
    }

    override fun enterGte(ctx: GteContext) {
      currentKeyvalue = CurrentKeyValue(comp = ComparisonOperator.GTE)
    }

    override fun enterEq(ctx: EqContext) {
      currentKeyvalue = CurrentKeyValue(comp = ComparisonOperator.EQ)
    }

    override fun exitKeyvalue(ctx: KeyvalueContext) {
      val kvqp = KeyValueQueryPart(currentKeyvalue!!.key!!,
        currentKeyvalue!!.value!!, currentKeyvalue!!.comp!!)
      val q = makeQuery(kvqp)
      result.first().add(q)
      currentKeyvalue = null
    }
  }
}
