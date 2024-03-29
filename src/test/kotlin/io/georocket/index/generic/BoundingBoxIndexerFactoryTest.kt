package io.georocket.index.generic

import io.georocket.query.GeoIntersects
import io.georocket.query.IndexQuery
import io.georocket.query.QueryCompiler.MatchPriority
import io.georocket.query.StringQueryPart
import org.assertj.core.api.Assertions.assertThat
import org.geotools.referencing.CRS
import org.junit.jupiter.api.Test

/**
 * Test the [BoundingBoxIndexerFactory]
 * @author Michel Kraemer
 */
class BoundingBoxIndexerFactoryTest {
  /**
   * Test if the factory returns NONE for invalid queries
   */
  @Test
  fun testInvalid() {
    val factory = BoundingBoxIndexerFactory()
    assertThat(factory.getQueryPriority(StringQueryPart(""))).isEqualTo(MatchPriority.NONE)
    assertThat(factory.getQueryPriority(StringQueryPart("42"))).isEqualTo(MatchPriority.NONE)
  }

  /**
   * Test if the factory compiles simple queries
   */
  @Test
  fun testQuery() {
    val point = "3477534.683,5605739.857"
    val query = StringQueryPart("$point,$point")
    val factory = BoundingBoxIndexerFactory()

    assertThat(factory.getQueryPriority(query)).isEqualTo(MatchPriority.ONLY)

    val destination = listOf(3477534.683, 5605739.857, 3477534.683, 5605739.857)
    testQuery(factory.compileQuery(query), destination)
  }

  /**
   * Test if the factory compiles EPSG queries
   */
  @Test
  fun testEPSG() {
    val point = "3477534.683,5605739.857"
    val query = "EPSG:31467:$point,$point"
    val queryPart = StringQueryPart(query)
    val lowerQueryPart = StringQueryPart(query.lowercase())
    val factory = BoundingBoxIndexerFactory()

    assertThat(factory.getQueryPriority(queryPart)).isEqualTo(MatchPriority.ONLY)
    assertThat(factory.getQueryPriority(lowerQueryPart)).isEqualTo(MatchPriority.ONLY)

    val destination = listOf(8.681739535269804, 50.58691850210496,
      8.681739535269804, 50.58691850210496)
    testQuery(factory.compileQuery(queryPart), destination)
    testQuery(factory.compileQuery(lowerQueryPart), destination)
  }

  /**
   * Test if the factory uses the configured default CRS code
   */
  @Test
  fun testEPSGDefault() {
    val point = "3477534.683,5605739.857"
    val query = "$point,$point"
    val queryPart = StringQueryPart(query)
    val lowerQueryPart = StringQueryPart(query.lowercase())
    val factory = BoundingBoxIndexerFactory("EPSG:31467")

    assertThat(factory.getQueryPriority(queryPart)).isEqualTo(MatchPriority.ONLY)
    assertThat(factory.getQueryPriority(lowerQueryPart)).isEqualTo(MatchPriority.ONLY)

    val destination = listOf(8.681739535269804, 50.58691850210496,
      8.681739535269804, 50.58691850210496)
    testQuery(factory.compileQuery(queryPart), destination)
    testQuery(factory.compileQuery(lowerQueryPart), destination)
  }

  /**
   * Test if CRS codes in queries have priority over the configured default CRS
   */
  @Test
  fun testEPSGDefaultQueryOverride() {
    val point = "3477534.683,5605739.857"
    val query = StringQueryPart("EPSG:31467:$point,$point")
    val factory = BoundingBoxIndexerFactory("invalid string")

    assertThat(factory.getQueryPriority(query)).isEqualTo(MatchPriority.ONLY)

    val destination = listOf(8.681739535269804, 50.58691850210496,
      8.681739535269804, 50.58691850210496)
    testQuery(factory.compileQuery(query), destination)
  }

  /**
   * Test if the factory uses the configured default CRS WKT
   */
  @Test
  fun testWKTDefault() {
    val wkt = CRS.decode("epsg:31467").toWKT()
    val point = "3477534.683,5605739.857"
    val query = StringQueryPart("$point,$point")
    val factory = BoundingBoxIndexerFactory(wkt)

    val destination = listOf(8.681739535269804, 50.58691850210496,
      8.681739535269804, 50.58691850210496)
    testQuery(factory.compileQuery(query), destination)
  }

  /**
   * Test if [query] contains correct [coordinates]
   */
  private fun testQuery(query: IndexQuery?, coordinates: List<Double>) {
    assertThat(query).isNotNull
    assertThat(query).isInstanceOf(GeoIntersects::class.java)

    val giq = query as GeoIntersects

    val jsonCoords = giq.geometry
      .getJsonArray("coordinates")
      .getJsonArray(0)

    val first = jsonCoords.getJsonArray(0)
    assertThat(first.getDouble(0)).isEqualTo(coordinates[0])
    assertThat(first.getDouble(1)).isEqualTo(coordinates[1])

    val second = jsonCoords.getJsonArray(1)
    assertThat(second.getDouble(0)).isEqualTo(coordinates[2])
    assertThat(second.getDouble(1)).isEqualTo(coordinates[3])
  }
}
