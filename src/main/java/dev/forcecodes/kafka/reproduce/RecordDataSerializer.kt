package dev.forcecodes.kafka.reproduce

import org.apache.kafka.common.serialization.Serializer

@Suppress("unused")
class RecordDataSerializer : Serializer<RecordData> {
  override fun serialize(p0: String?, p1: RecordData?): ByteArray {
    return p1.toString().encodeToByteArray()
  }
}