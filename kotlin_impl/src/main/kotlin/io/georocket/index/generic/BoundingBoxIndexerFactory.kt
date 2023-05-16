package io.georocket.index.generic

import io.georocket.constants.ConfigConstants
import io.georocket.index.DatabaseIndex
import io.georocket.index.Indexer
import io.georocket.index.IndexerFactory
import io.georocket.index.geojson.GeoJsonBoundingBoxIndexer
import io.georocket.index.xml.XMLBoundingBoxIndexer
import io.georocket.query.GeoIntersects
import io.georocket.query.IndexQuery
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.QueryPart
import io.georocket.query.StringQueryPart
import io.georocket.util.CoordinateTransformer
import io.georocket.util.JsonStreamEvent
import io.georocket.util.StreamEvent
import io.georocket.util.XMLStreamEvent
import io.vertx.core.Vertx
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import org.geotools.referencing.CRS

/**
 * Base class for factories creating indexers that manage bounding boxes
 * @author Michel Kraemer
 */
class BoundingBoxIndexerFactory : IndexerFactory {
  companion object {
    private const val FLOAT_REGEX = "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?"
    private const val COMMA_REGEX = "\\s*,\\s*"
    private const val CODE_PREFIX = "([a-zA-Z]+:\\d+:)?"
    private val BBOX_REGEX = (CODE_PREFIX + FLOAT_REGEX + COMMA_REGEX +
      FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX).toRegex()
  }

  val defaultCrs: String?

  /**
   * Default constructor
   */
  constructor() {
    defaultCrs = Vertx.currentContext()
      ?.config()
      ?.getString(ConfigConstants.QUERY_DEFAULT_CRS)
  }

  /**
   * Construct a new instance with an explicit defaultCrs
   */
  constructor(defaultCrs: String?) {
    this.defaultCrs = defaultCrs
  }

  @Suppress("UNCHECKED_CAST")
  override fun <T : StreamEvent> createIndexer(eventType: Class<T>): Indexer<T>? {
    if (eventType.isAssignableFrom(XMLStreamEvent::class.java)) {
      return XMLBoundingBoxIndexer() as Indexer<T>
    } else if (eventType.isAssignableFrom(JsonStreamEvent::class.java)) {
      return GeoJsonBoundingBoxIndexer() as Indexer<T>
    }
    return null
  }

  override fun getQueryPriority(queryPart: QueryPart): MatchPriority {
    return when (queryPart) {
      is StringQueryPart -> if (BBOX_REGEX.matches(queryPart.value)) {
        MatchPriority.ONLY
      } else {
        MatchPriority.NONE
      }

      else -> MatchPriority.NONE
    }
  }

  override fun compileQuery(queryPart: QueryPart): IndexQuery? {
    if (queryPart !is StringQueryPart) {
      return null
    }

    val index = queryPart.value.lastIndexOf(':')
    val (crsCode, co) = if (index > 0) {
      queryPart.value.substring(0, index) to queryPart.value.substring(index + 1)
    } else {
      null to queryPart.value
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
      points = transformer.transform(points, 2)!!
    }

    val minX = points[0]
    val minY = points[1]
    val maxX = points[2]
    val maxY = points[3]

    return GeoIntersects(
      "bbox", jsonObjectOf(
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
  }

  override fun getDatabaseIndexes(indexedFields: List<String>): List<DatabaseIndex> = listOf(
    DatabaseIndex.Geo("bbox", "bbox_geo")
  )
}
