package io.georocket.index.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import io.georocket.index.generic.BoundingBoxIndexerFactory;

/**
 * Create instances of {@link XMLBoundingBoxIndexer}
 * @author Michel Kraemer
 */
public class XMLBoundingBoxIndexerFactory extends BoundingBoxIndexerFactory
    implements XMLIndexerFactory {
  private static final String PROPERTIES_FILENAME =
    "io.georocket.index.xml.XMLBoundingBoxIndexerFactory.properties";
  private static final String BOUNDINGBOXINDEXER_CLASSNAME =
    "io.georocket.index.xml.XMLBoundingBoxIndexer";
  private static Class<? extends XMLBoundingBoxIndexer> configuredClass;

  static {
    loadConfig();
  }

  @SuppressWarnings("unchecked")
  private static void loadConfig() {
    try (InputStream is = XMLBoundingBoxIndexerFactory.class.getClassLoader()
        .getResourceAsStream(PROPERTIES_FILENAME)) {
      if (is != null) {
        Properties p = new Properties();
        p.load(is);
        String cls = p.getProperty(BOUNDINGBOXINDEXER_CLASSNAME);
        if (cls != null) {
          try {
            configuredClass = (Class<? extends XMLBoundingBoxIndexer>)Class.forName(cls);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not find custom "
              + "BoundingBoxIndexer class: " + cls, e);
          }
        }
      }
    } catch (IOException e) {
      // ignore
    }
  }

  @Override
  public XMLIndexer createIndexer() {
    if (configuredClass == null) {
      return new XMLBoundingBoxIndexer();
    }
    try {
      return configuredClass.newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not create custom "
        + "BoundingBoxIndexer", e);
    }
  }
}
