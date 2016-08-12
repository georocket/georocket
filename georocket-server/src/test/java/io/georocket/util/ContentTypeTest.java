package io.georocket.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for the ContentType class.
 *
 * @author Andrej Sajenko
 */
public class ContentTypeTest {

  @Test
  public void testParseSimpleType() {
    String type = "application/xml";

    ContentType cb = ContentType.parse(type);

    assertEquals("application", cb.getType());
    assertEquals("xml", cb.getSubtype());
    assertEquals("", cb.getSpecificType());
    assertEquals(0, cb.getParameters().size());
  }

  @Test
  public void testParseSpecificType() {
    String type = "application/gml+xml";

    ContentType cb = ContentType.parse(type);

    assertEquals("application", cb.getType());
    assertEquals("xml", cb.getSubtype());
    assertEquals("gml", cb.getSpecificType());
    assertEquals(0, cb.getParameters().size());
  }

  @Test
  public void testParseSimpleTypeWithParams1() {
    String type = "application/xml; charset=utf-8";

    ContentType cb = ContentType.parse(type);

    assertEquals("application", cb.getType());
    assertEquals("xml", cb.getSubtype());
    assertEquals("", cb.getSpecificType());
    assertEquals(1, cb.getParameters().size());
    assertEquals("utf-8", cb.getParameters().get("charset"));
  }

  @Test
  public void testParseSimpleTypeWithParams2() {
    String type = "application/xml; charset=utf-8;version=\"1.0\"";

    ContentType cb = ContentType.parse(type);

    assertEquals("application", cb.getType());
    assertEquals("xml", cb.getSubtype());
    assertEquals("", cb.getSpecificType());
    assertEquals(2, cb.getParameters().size());
    assertEquals("utf-8", cb.getParameters().get("charset"));
    assertEquals("1.0", cb.getParameters().get("version"));
  }

  @Test
  public void testParseSpecificTypeWithParams() {
    String type = "application/gml+xml; charset=utf-8;version=\"1.0\"";

    ContentType cb = ContentType.parse(type);

    assertEquals("application", cb.getType());
    assertEquals("xml", cb.getSubtype());
    assertEquals("gml", cb.getSpecificType());
    assertEquals(2, cb.getParameters().size());
    assertEquals("utf-8", cb.getParameters().get("charset"));
    assertEquals("1.0", cb.getParameters().get("version"));
  }

  @Test
  public void testBelongsToContentType() {
    ContentType type = ContentType.parse("application/xml");

    assertTrue(type.belongsToContentType("application/xml"));
    assertFalse(type.belongsToContentType("application/json"));
  }

  @Test
  public void testBelongsToContentTypeSpecific() {
    ContentType type = ContentType.parse("application/gml+xml");

    assertTrue(type.belongsToContentType("application/gml+xml"));
    assertTrue(type.belongsToContentType("application/xml"));
  }

  @Test
  public void testBelongsToContentTypeNot() {
    ContentType type = ContentType.parse("application/xml");

    assertFalse(type.belongsToContentType("application/gml+xml"));
  }
}
