package io.georocket.index

import org.antlr.v4.runtime.misc.ParseCancellationException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Tests [PropertiesParser]
 * @author Michel Kraemer
 */
class PropertiesParserTest {
  /**
   * Parse a simple key-value pair
   */
  @Test
  fun simple() {
    val r = PropertiesParser.parse("key:value")
    assertThat(r).isEqualTo(mapOf("key" to "value"))
  }

  /**
   * Parse a key-value pair with quotes
   */
  @Test
  fun quoted() {
    val r1 = PropertiesParser.parse("\"foo bar\":value")
    assertThat(r1).isEqualTo(mapOf("foo bar" to "value"))

    val r2 = PropertiesParser.parse("key:\"value with spaces\"")
    assertThat(r2).isEqualTo(mapOf("key" to "value with spaces"))

    val r3 = PropertiesParser.parse("'foo bar':'value with spaces'")
    assertThat(r3).isEqualTo(mapOf("foo bar" to "value with spaces"))
  }

  /**
   * Parse a key-value pair with colons in quotes
   */
  @Test
  fun colonInQuotes() {
    val r1 = PropertiesParser.parse("\"foo:bar\":value")
    assertThat(r1).isEqualTo(mapOf("foo:bar" to "value"))

    val r2 = PropertiesParser.parse("key:\"value:with:colons\"")
    assertThat(r2).isEqualTo(mapOf("key" to "value:with:colons"))
  }

  /**
   * Parse multiple key-value pairs
   */
  @Test
  fun multiple() {
    val r1 = PropertiesParser.parse("key:value,foo:bar,another:value")
    assertThat(r1).isEqualTo(mapOf("key" to "value", "foo" to "bar", "another" to "value"))
  }

  /**
   * A complex example
   */
  @Test
  fun complex() {
    val r1 = PropertiesParser.parse("key:value,'my name is':elvis,\"a key:with,colon,and:comma\":'crazy value,with:comma,and:colon'")
    assertThat(r1).isEqualTo(mapOf("key" to "value", "my name is" to "elvis",
      "a key:with,colon,and:comma" to "crazy value,with:comma,and:colon"))
  }

  @Test
  fun error() {
    assertThatThrownBy {
      PropertiesParser.parse("key:")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      PropertiesParser.parse("key:value,")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      PropertiesParser.parse("k\"ey\":value")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      PropertiesParser.parse("\"key:value")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      PropertiesParser.parse("")
    }.isInstanceOf(ParseCancellationException::class.java)
  }
}
