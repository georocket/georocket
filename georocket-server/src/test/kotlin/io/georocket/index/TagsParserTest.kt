package io.georocket.index

import org.antlr.v4.runtime.misc.ParseCancellationException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Tests [TagsParser]
 * @author Michel Kraemer
 */
class TagsParserTest {
  /**
   * Parse a simple tag
   */
  @Test
  fun simple() {
    val r = TagsParser.parse("tag")
    assertThat(r).isEqualTo(listOf("tag"))
  }

  /**
   * Parse a quoted tag
   */
  @Test
  fun quoted() {
    val r1 = TagsParser.parse("\"foo bar\"")
    assertThat(r1).isEqualTo(listOf("foo bar"))

    val r2 = TagsParser.parse("'foo bar'")
    assertThat(r2).isEqualTo(listOf("foo bar"))
  }

  /**
   * Parse a tag with colons
   */
  @Test
  fun colonInQuotes() {
    val r1 = TagsParser.parse("\"foo:bar\"")
    assertThat(r1).isEqualTo(listOf("foo:bar"))
  }

  /**
   * Parse multiple tags
   */
  @Test
  fun multiple() {
    val r1 = TagsParser.parse("tag,foo,bar,another")
    assertThat(r1).isEqualTo(listOf("tag", "foo", "bar", "another"))
  }

  /**
   * A complex example
   */
  @Test
  fun complex() {
    val r1 =
      TagsParser.parse("tag,'my name is:elvis',\"a tag:with,colon,and:comma\"")
    assertThat(r1).isEqualTo(listOf("tag", "my name is:elvis",
      "a tag:with,colon,and:comma"))
  }

  @Test
  fun error() {
    assertThatThrownBy {
      TagsParser.parse("tag,")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      TagsParser.parse("t\"ag\"")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      TagsParser.parse("\"tag")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      TagsParser.parse("")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      TagsParser.parse("tag\nwithlinebreak")
    }.isInstanceOf(ParseCancellationException::class.java)

    assertThatThrownBy {
      TagsParser.parse("\"tag\nwithlinebreak\"")
    }.isInstanceOf(ParseCancellationException::class.java)
  }
}
