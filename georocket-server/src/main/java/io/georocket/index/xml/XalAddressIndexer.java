package io.georocket.index.xml;

import com.google.common.collect.ImmutableMap;
import io.georocket.util.XMLStreamEvent;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Indexes XAL 2.0 addresses. Currently it supports the following structure:</p>
 *
 * <pre>
 * &lt;xal:AddressDetails>
 *   &lt;xal:Country>
 *     &lt;xal:CountryName>My Country&lt;/xal:CountryName>
 *     &lt;xal:Locality Type="Town">
 *       &lt;xal:LocalityName>My City&lt;/xal:LocalityName>
 *       &lt;xal:Thoroughfare Type="Street">
 *         &lt;xal:ThoroughfareName>My Street&lt;/xal:ThoroughfareName>
 *         &lt;xal:ThoroughfareNumber>1&lt;/xal:ThoroughfareNumber>
 *       &lt;/xal:Thoroughfare>
 *     &lt;/xal:Locality>
 *   &lt;/xal:Country>
 * &lt;/xal:AddressDetails>
 * </pre>
 *
 * <p>The following attributes will be extracted from this structure:</p>
 *
 * <pre>
 * {
 *   "Country": "My Country",
 *   "Locality": "My City",
 *   "Street": "My Street",
 *   "Number": "1"
 * }
 * </pre>
 *
 * <p>Note that XAL specifies a lot more attributes that are not supported at
 * the moment but will probably be in future versions.</p>
 *
 * @author Michel Kraemer
 */
public class XalAddressIndexer implements XMLIndexer {
  private static final String NS_XAL = "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0";

  private String currentKey;
  private Map<String, String> result = new HashMap<>();

  @Override
  public void onEvent(XMLStreamEvent event) {
    int e = event.getEvent();
    if (e == XMLEvent.START_ELEMENT) {
      XMLStreamReader reader = event.getXMLReader();
      if (NS_XAL.equals(reader.getNamespaceURI())) {
        switch (reader.getLocalName()) {
          case "CountryName":
            currentKey = "Country";
            break;

          case "LocalityName":
            currentKey = "Locality";
            break;

          case "ThoroughfareName":
            currentKey = "Street";
            break;

          case "ThoroughfareNumber":
            currentKey = "Number";
            break;
        }
      }
    } else if (e == XMLEvent.END_ELEMENT) {
      currentKey = null;
    } else if (e == XMLEvent.CHARACTERS && currentKey != null) {
      String value = event.getXMLReader().getText();
      if (value != null) {
        value = value.trim();
        if (!value.isEmpty()) {
          result.put(currentKey, value);
        }
      }
    }
  }

  @Override
  public Map<String, Object> getResult() {
    return ImmutableMap.of("address", result);
  }
}
