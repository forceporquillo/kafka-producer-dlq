package dev.forcecodes.kafka.reproduce

fun interface KafkaReadyListener {

  @Throws(Exception::class)
  fun onKafkaReady(topics: List<String>)
}