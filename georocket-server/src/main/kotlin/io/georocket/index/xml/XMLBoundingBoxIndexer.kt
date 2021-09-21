package io.georocket.index.xml

import io.georocket.index.CRSAware
import io.georocket.index.generic.BoundingBoxIndexer
import io.georocket.util.CompoundCRSDecoder
import io.georocket.util.XMLStreamEvent
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS.WGS84
import org.opengis.referencing.FactoryException
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.cs.AxisDirection
import org.opengis.referencing.operation.MathTransform
import org.opengis.referencing.operation.TransformException
import javax.xml.stream.events.XMLEvent

/**
 * Indexes bounding boxes of inserted chunks
 * @author Michel Kraemer
 */
class XMLBoundingBoxIndexer : BoundingBoxIndexer(), XMLIndexer, CRSAware {
  /**
   * The string of the detected CRS
   */
  private var crsStr: String? = null

  /**
   * The detected CRS or `null` if either there was no CRS in the
   * chunk or if it was invalid
   */
  private var crs: CoordinateReferenceSystem? = null

  /**
   * True if x and y are flipped in [crs]
   */
  private var flippedCRS = false

  /**
   * A transformation from [crs] to WGS84. `null` if [crs] is also `null`.
   */
  private var transform: MathTransform? = null

  /**
   * True if we're currently parsing a `<lowerCorner>` or
   * `<upperCorner>` element
   */
  private var parseCorner = false

  /**
   * True if we're currently parsing a `<posList>` element
   */
  private var parsePosList = false

  /**
   * The spatial dimension of the element we're currently parsing or
   * `-1` if it could not be determined or if we are currently
   * not parsing an element with spatial content.
   */
  private var srsDimension = -1

  /**
   * Collects XML element contents
   */
  private val stringBuilder = StringBuilder()

  /**
   * Check if x and y are flipped in the given [crs]
   */
  private fun isFlippedCRS(crs: CoordinateReferenceSystem): Boolean {
    if (crs.coordinateSystem.dimension == 2) {
      val direction = crs.coordinateSystem.getAxis(0).direction
      if (direction == AxisDirection.NORTH || direction == AxisDirection.UP) {
        return true
      }
    }
    return false
  }

  override fun setFallbackCRSString(crsStr: String) {
    handleSrsName(crsStr)
  }

  override fun onEvent(event: XMLStreamEvent) {
    val reader = event.xmlReader
    if (event.event == XMLEvent.START_ELEMENT) {
      // parse SRS and try to decode it
      handleSrsName(reader.getAttributeValue(null, "srsName"))

      // check if we've got an element containing spatial coordinates
      when (reader.localName) {
        "Envelope" -> {
          // try to parse the spatial dimension of this envelope
          handleSrsDimension(reader.getAttributeValue(null, "srsDimension"))
        }

        "lowerCorner", "upperCorner" -> {
          // lower and upper corner of an envelope
          parseCorner = true
          stringBuilder.clear()
        }

        "posList", "pos" -> {
          // list of positions of a GML geometry
          parsePosList = true
          stringBuilder.clear()
          handleSrsDimension(reader.getAttributeValue(null, "srsDimension"))
        }
      }
    } else if (event.event == XMLEvent.CHARACTERS) {
      if (parseCorner || parsePosList) {
        // collect contents and parse them when we encounter the end element
        stringBuilder.append(reader.text)
      }
    } else if (event.event == XMLEvent.END_ELEMENT) {
      when (reader.localName) {
        "Envelope" -> {
          srsDimension = -1
        }

        "lowerCorner", "upperCorner" -> {
          handlePosList(stringBuilder.toString())
          parseCorner = false
        }

        "posList", "pos" -> {
          handlePosList(stringBuilder.toString())
          parsePosList = false
          srsDimension = -1
        }
      }
    }
  }

  /**
   * Parse and decode a [srsName]
   */
  private fun handleSrsName(srsName: String?) {
    if (srsName == null || srsName.isEmpty()) {
      return
    }

    if (crsStr != null && crsStr == srsName) {
      // same string, no need to parse
      return
    }

    try {
      // decode string
      var crs = if (CompoundCRSDecoder.isCompound(srsName)) {
        CompoundCRSDecoder.decode(srsName)
      } else {
        CRS.decode(srsName)
      }

      // only keep horizontal CRS
      val hcrs = CRS.getHorizontalCRS(crs)
      if (hcrs != null) {
        crs = hcrs
      }

      // find transformation to WGS84
      val transform = CRS.findMathTransform(crs, WGS84, true)
      val flippedCRS = isFlippedCRS(crs)
      crsStr = srsName
      this.crs = crs
      this.transform = transform
      this.flippedCRS = flippedCRS
    } catch (e: FactoryException) {
      // unknown CRS or no transformation available
    }
  }

  /**
   * Parse the [srsDimension] attribute
   */
  private fun handleSrsDimension(srsDimension: String?) {
    if (srsDimension == null || srsDimension.isEmpty()) {
      this.srsDimension = -1
      return
    }

    try {
      this.srsDimension = srsDimension.toInt()
    } catch (e: NumberFormatException) {
      this.srsDimension = -1
    }
  }

  /**
   * Handle the contents of a `posList` element. Parses all
   * coordinates and adds them to the bounding box.
   */
  private fun handlePosList(text: String?) {
    if (crs == null || text == null || text.isEmpty()) {
      return
    }
    val coordinates = text.trim().split("""\s+""".toRegex())

    // check if dimension is correct
    val dim = if (srsDimension <= 0) {
      if (coordinates.size % 3 == 0) {
        3
      } else if (coordinates.size % 2 == 0) {
        2
      } else {
        // unknown dimension. ignore this pos list
        return
      }
    } else {
      srsDimension
    }

    // convert strings to doubles
    val count = coordinates.size / dim
    val srcCoords = DoubleArray(count * 2)
    val dstCoords = DoubleArray(count * 2)
    try {
      for (i in 0 until count) {
        var j1 = i * 2
        var j2 = j1 + 1
        if (flippedCRS) {
          j2 = j1
          j1 = j2 + 1
        }
        srcCoords[j1] = coordinates[i * dim].toDouble()
        srcCoords[j2] = coordinates[i * dim + 1].toDouble()
      }
    } catch (e: NumberFormatException) {
      // invalid coordinates. ignore.
      return
    }
    try {
      // transform coordinates to WGS84
      transform!!.transform(srcCoords, 0, dstCoords, 0, count)
      for (i in 0 until count) {
        // add coordinates to bounding box
        addToBoundingBox(dstCoords[i * 2], dstCoords[i * 2 + 1])
      }
    } catch (e: TransformException) {
      // ignore
    }
  }
}
