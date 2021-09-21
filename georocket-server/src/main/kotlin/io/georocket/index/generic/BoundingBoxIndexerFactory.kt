package io.georocket.index.generic

import io.georocket.constants.ConfigConstants
import io.georocket.index.IndexerFactory
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.util.CoordinateTransformer
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import org.geotools.referencing.CRS

/**
 * Base class for factories creating indexers that manage bounding boxes
 * @author Michel Kraemer
 */
abstract class BoundingBoxIndexerFactory : IndexerFactory {
  companion object {
    private const val FLOAT_REGEX = "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?"
    private const val COMMA_REGEX = "\\s*,\\s*"
    private const val CODE_PREFIX = "([a-zA-Z]+:\\d+:)?"
    private val BBOX_REGEX = (CODE_PREFIX + FLOAT_REGEX + COMMA_REGEX +
        FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX).toRegex()
  }

  var defaultCrs: String? = null

  /**
   * Default constructor
   */
  init {
    val ctx = Vertx.currentContext()
    if (ctx != null) {
      val config = ctx.config()
      if (config != null) {
        defaultCrs = config.getString(ConfigConstants.QUERY_DEFAULT_CRS)
      }
    }
  }

  override fun getQueryPriority(search: String): MatchPriority {
    return if (BBOX_REGEX.matches(search)) {
      MatchPriority.ONLY
    } else {
      MatchPriority.NONE
    }
  }

  override fun compileQuery(search: String): JsonObject {
    val index = search.lastIndexOf(':')
    val (crsCode, co) = if (index > 0) {
      search.substring(0, index) to search.substring(index + 1)
    } else {
      null to search
    }

    val crs = if (crsCode != null) {
      CRS.decode(crsCode)
    } else if (defaultCrs != null) {
      CoordinateTransformer.decode(defaultCrs)
    } else {
      null
    }

    var points = co.split(",").map { it.trim().toDouble() }.toDoubleArray()
    if (crs != null) {
      val transformer = CoordinateTransformer(crs)
      points = transformer.transform(points, -1)
    }

    val minX = points[0]
    val minY = points[1]
    val maxX = points[2]
    val maxY = points[3]

    return jsonObjectOf("bbox" to jsonObjectOf(
      "\$geoIntersects" to jsonObjectOf(
        "\$geometry" to jsonObjectOf(
          "type" to "Polygon",
          "coordinates" to jsonArrayOf(
            jsonArrayOf(
              jsonArrayOf(minX, minY),
              jsonArrayOf(maxX, minY),
              jsonArrayOf(maxX, maxY),
              jsonArrayOf(minX, maxY),
              jsonArrayOf(minX, minY)
            )
          )
        )
      )
    ))
  }
}
