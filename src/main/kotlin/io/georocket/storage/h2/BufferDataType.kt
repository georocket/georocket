package io.georocket.storage.h2

import io.vertx.core.buffer.Buffer
import org.h2.mvstore.DataUtils
import org.h2.mvstore.WriteBuffer
import org.h2.mvstore.type.BasicDataType
import java.nio.ByteBuffer

class BufferDataType : BasicDataType<Buffer>() {
  override fun compare(a: Buffer, b: Buffer): Int {
    return a.byteBuf.compareTo(b.byteBuf)
  }

  override fun getMemory(obj: Buffer): Int {
    return 24 + obj.length()
  }

  override fun write(buff: WriteBuffer, obj: Buffer) {
    buff.putVarInt(obj.length()).put(obj.bytes)
  }

  override fun read(buff: ByteBuffer): Buffer {
    val len = DataUtils.readVarInt(buff)
    val b = ByteArray(len)
    buff.get(b)
    return Buffer.buffer(b)
  }

  override fun createStorage(size: Int): Array<Buffer?> {
    return arrayOfNulls(size)
  }
}
