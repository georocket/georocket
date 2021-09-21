package io.georocket.index.xml

import io.georocket.util.XMLStreamEvent
import javax.xml.stream.events.XMLEvent

/**
 * Indexes XAL 2.0 addresses. Currently, it supports the following structure:
 *
 *     <xal:AddressDetails>
 *       <xal:Country>
 *         <xal:CountryName>My Country</xal:CountryName>
 *         <xal:Locality Type="Town">
 *           <xal:LocalityName>My City</xal:LocalityName>
 *           <xal:Thoroughfare Type="Street">
 *             <xal:ThoroughfareName>My Street</xal:ThoroughfareName>
 *             <xal:ThoroughfareNumber>1</xal:ThoroughfareNumber>
 *           </xal:Thoroughfare>
 *         </xal:Locality>
 *       </xal:Country>
 *     </xal:AddressDetails>
 *
 * The following attributes will be extracted from this structure:
 *
 *     {
 *       "Country": "My Country",
 *       "Locality": "My City",
 *       "Street": "My Street",
 *       "Number": "1"
 *     }
 *
 * Note that XAL specifies a lot more attributes that are not supported at
 * the moment but will probably be in future versions.
 *
 * @author Michel Kraemer
 */
class XalAddressIndexer : XMLIndexer {
  companion object {
    private const val NS_XAL = "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0"

    enum class Keys(val key: String) {
      COUNTRY("Country"),
      LOCALITY("Locality"),
      STREET("Street"),
      NUMBER("Number")
    }
  }

  private var currentKey: String? = null
  private val result = mutableMapOf<String, String>()

  override fun onEvent(event: XMLStreamEvent) {
    val e = event.event
    if (e == XMLEvent.START_ELEMENT) {
      val reader = event.xmlReader
      if (NS_XAL == reader.namespaceURI) {
        when (reader.localName) {
          "CountryName" -> currentKey = Keys.COUNTRY.key
          "LocalityName" -> currentKey = Keys.LOCALITY.key
          "ThoroughfareName" -> currentKey = Keys.STREET.key
          "ThoroughfareNumber" -> currentKey = Keys.NUMBER.key
        }
      }
    } else if (e == XMLEvent.END_ELEMENT) {
      currentKey = null
    } else if (e == XMLEvent.CHARACTERS && currentKey != null) {
      var value = event.xmlReader.text
      if (value != null) {
        value = value.trim()
        if (value.isNotEmpty()) {
          result[currentKey!!] = value
        }
      }
    }
  }

  override fun getResult(): Map<String, Any> = mapOf("address" to result)
}
