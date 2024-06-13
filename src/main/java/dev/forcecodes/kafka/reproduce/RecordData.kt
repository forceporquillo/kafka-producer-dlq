package dev.forcecodes.kafka.reproduce

data class RecordData(val value: String, var correlationId: String = "")