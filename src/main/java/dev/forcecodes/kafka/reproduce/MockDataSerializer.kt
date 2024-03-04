package dev.forcecodes.kafka.reproduce

import org.apache.kafka.common.serialization.Serializer

@Suppress("unused")
class MockDataSerializer : Serializer<MockData> {
  override fun serialize(p0: String?, p1: MockData?): ByteArray {
    return p1.toString().encodeToByteArray()
  }
}