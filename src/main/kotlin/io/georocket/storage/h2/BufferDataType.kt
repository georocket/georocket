package io.georocket.storage.h2

import io.vertx.core.buffer.Buffer
import org.h2.mvstore.DataUtils
import org.h2.mvstore.WriteBuffer
import org.h2.mvstore.type.DataType
import java.nio.ByteBuffer

class BufferDataType : DataType {
  override fun compare(a: Any, b: Any): Int {
    val ab = a as Buffer
    val bb = b as Buffer
    return ab.byteBuf.compareTo(bb.byteBuf)
  }

  override fun getMemory(obj: Any): Int {
    return 24 + (obj as Buffer).length()
  }

  override fun write(buff: WriteBuffer, obj: Any) {
    val b = obj as Buffer
    buff.putVarInt(b.length()).put(b.bytes)
  }

  override fun write(buff: WriteBuffer, obj: Array<Any>, len: Int, key: Boolean) {
    for (i in 0 until len) {
      write(buff, obj[i])
    }
  }

  override fun read(buff: ByteBuffer): Buffer {
    val len = DataUtils.readVarInt(buff)
    val b = ByteArray(len)
    buff.get(b)
    return Buffer.buffer(b)
  }

  override fun read(buff: ByteBuffer, obj: Array<Any>, len: Int, key: Boolean) {
    for (i in 0 until len) {
      obj[i] = read(buff)
    }
  }
}
