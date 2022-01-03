package io.georocket.query

import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart.ComparisonOperator
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
import io.georocket.query.parser.QueryParser.NumberContext
import io.georocket.query.parser.QueryParser.OrContext
import io.georocket.query.parser.QueryParser.StringContext
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ParseTreeWalker

/**
 * Default implementation of [QueryCompiler]
 * @author Michel Kraemer
 */
class DefaultQueryCompiler(private val queryCompilers: Collection<QueryCompiler>) : QueryCompiler {
  /**
   * Compile a [search] string with an optional chunk [path]
   */
  fun compileQuery(search: String, path: String?): IndexQuery {
    var qb = compileQuery(search)
    if (path != null && path != "/") {
      qb = And(qb, StartsWith("path", path))
    }
    return qb
  }

  override fun compileQuery(queryPart: QueryPart): IndexQuery? {
    if (queryPart is StringQueryPart) {
      return compileQuery(queryPart.value)
    }
    return null
  }

  private fun compileQuery(search: String): IndexQuery {
    if (search.isEmpty()) {
      // match everything by default
      return All
    }

    // parse query
    val lexer = QueryLexer(CharStreams.fromString(search.trim()))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val ctx = parser.query()

    // compile query to QueryBuilder
    val listener = QueryCompilerListener()
    ParseTreeWalker.DEFAULT.walk(listener, ctx)

    val operands = listener.result.firstOrNull() ?: listOf(All)

    return if (operands.size == 1) {
      operands.first()
    } else {
      Or(operands)
    }
  }

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return MatchPriority.ONLY
  }

  /**
   * Handle a [queryPart]. Pass it to all query compilers and return a
   * MongoDB query.
   */
  private fun makeQuery(queryPart: QueryPart): IndexQuery? {
    val operands = mutableListOf<IndexQuery>()
    for (f in queryCompilers) {
      when (f.getQueryPriority(queryPart)) {
        MatchPriority.ONLY -> return f.compileQuery(queryPart)
        MatchPriority.SHOULD -> f.compileQuery(queryPart)?.let { operands.add(it) }
        MatchPriority.NONE -> { /* ignore operand */ }
      }
    }

    return if (operands.size > 1) {
      Or(operands)
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

  private data class CurrentKeyValue(val key: String? = null, val value: Any? = null,
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
    val result = ArrayDeque<MutableList<IndexQuery>>()

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
      val r = when (currentLogical.removeFirst()) {
        Logical.OR -> Or(result.removeFirst())
        Logical.AND -> And(result.removeFirst())
        Logical.NOT -> {
          val co = result.removeFirst()
          Not(if (co.size == 1) co.first() else Or(co))
        }
      }
      result.first().add(r)
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
        makeQuery(StringQueryPart(str))?.let { result.first().add(it) }
      }
    }

    override fun enterNumber(ctx: NumberContext) {
      val l = ctx.text.toLongOrNull()
      if (l != null) {
        if (currentKeyvalue != null) {
          currentKeyvalue = currentKeyvalue?.copy(value = l)
        } else {
          makeQuery(LongQueryPart(l))?.let { result.first().add(it) }
        }
      } else {
        val d = ctx.text.toDouble()
        if (currentKeyvalue != null) {
          currentKeyvalue = currentKeyvalue?.copy(value = d)
        } else {
          makeQuery(DoubleQueryPart(d))?.let { result.first().add(it) }
        }
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
      val cv = currentKeyvalue!!
      val kvqp = when (cv.value) {
        is String -> StringQueryPart(cv.value, cv.key, cv.comp)
        is Long -> LongQueryPart(cv.value, cv.key, cv.comp)
        is Double -> DoubleQueryPart(cv.value, cv.key, cv.comp)
        else -> throw RuntimeException("Illegal value type `${cv.value?.javaClass}'")
      }
      makeQuery(kvqp)?.let { result.first().add(it) }
      currentKeyvalue = null
    }
  }
}
