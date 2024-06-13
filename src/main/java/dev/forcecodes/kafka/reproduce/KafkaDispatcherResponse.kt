package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.producer.RecordMetadata

class KafkaDispatcherResponse<T> private constructor(
  val key: String?,
  val payload: T?,
  val metadata: RecordMetadata?,
  val exception: Exception?,
  private val state: State? = null
) {

  enum class State(val key: String) {
    SUCCESS("sent"),
    FAILURE("fail"),
    RETRY("retry"),
    POLLED("polled")
  }

  // Builder class
  class Builder<T> {

    private var key: String? = null
    private var payload: T? = null
    private var metadata: RecordMetadata? = null
    private var exception: Exception? = null
    private var state: State? = null

    fun setKey(key: String): Builder<T> {
      this.key = key
      return this
    }

    fun setPayload(payload: T): Builder<T> {
      this.payload = payload
      return this
    }

    fun setMetadata(metadata: RecordMetadata?): Builder<T> {
      this.metadata = metadata
      return this
    }

    fun setException(exception: Exception?): Builder<T> {
      this.exception = exception
      return this
    }

    fun setState(state: State?): Builder<T> {
      this.state = state
      return this
    }

    fun build(): KafkaDispatcherResponse<T> {
      return KafkaDispatcherResponse(key, payload, metadata, exception, state)
    }
  }

  fun state(): State {
    return state
      ?: if (exception == null) State.SUCCESS else State.FAILURE
  }

  override fun toString(): String {
    return "KafkaDispatcherResponse(key=$key, payload=$payload, metadata=$metadata, exception=$exception, state=$state)"
  }


}
