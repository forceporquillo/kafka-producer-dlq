package dev.forcecodes.kafka.reproduce

fun interface KafkaDispatcherCallback<T> {

  fun onDispatch(response: KafkaDispatcherResponse<T>)
}
