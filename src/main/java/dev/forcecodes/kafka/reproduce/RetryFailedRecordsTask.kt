package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.logging.log4j.kotlin.logger
import java.util.concurrent.BlockingQueue

class RetryFailedRecordsTask<O>(builder: Builder<O>) : Runnable {

  private val logger = logger()

  private val correlationIdUpdater: EventCorrelationIdUpdater<O>?
  private val recordQueue: BlockingQueue<ProducerRecord<String, O>>?
  private val adminClient: KafkaAdminClient?
  private val topicName: String?
  private val producerHandle: KafkaProducer<String, O>?
  private val dispatcherCallback: KafkaDispatcherCallback<O>?
  private var isCancelled = false

  init {
    topicName = builder.topicName
    adminClient = builder.adminClient
    recordQueue = builder.recordQueue
    producerHandle = builder.producerHandle
    dispatcherCallback = builder.dispatcherCallback
    correlationIdUpdater = builder.correlationIdUpdater
  }

  override fun run() {
    do {
      if (recordQueue!!.isEmpty()) {
        continue
      }
      logger.debug(
        "There are at least ${recordQueue.size} " +
            "${if (recordQueue.size > 1) "messages" else "message"} in the queue record bucket. " +
            "Will attempt to re-schedule processing all records to the [$topicName] topic " +
            "once broker became available",
      )
      val isAvailable = adminClient?.getAvailableTopics(true) { topics ->
        logger.debug(
          "Kafka broker is up and running with listed topics ${
            topics.toTypedArray().contentToString()
          }"
        )
      }
      if (isAvailable == true) {
        processRecordsFromDlq()
      }
    } while (!isCancelled)
  }

  private fun processRecordsFromDlq() {
    try {
      var builder: KafkaDispatcherResponse.Builder<O>
      recordQueue?.let { queue ->
        while (queue.isNotEmpty()) {
          val record = queue.take()
          correlationIdUpdater?.requestUpdate(record.value())
          builder = KafkaDispatcherResponse.Builder<O>()
            .setKey(record.key())
            .setPayload(record.value())
          producerHandle?.send(record) { recordMetadata, exception ->
            // dispatch only when Kafka broker acknowledges the message
            dispatcherCallback?.apply {
              if (exception == null) {
                onDispatch(
                  builder.setMetadata(recordMetadata)
                    .setState(KafkaDispatcherResponse.State.SUCCESS)
                    .build()
                )
              }
            }
          }?.get() // block thread
        }
      }
      builder = KafkaDispatcherResponse.Builder<O>().setState(KafkaDispatcherResponse.State.POLLED)
      dispatcherCallback?.onDispatch(builder.build())
    } catch (e: Exception) {
      logger.error(e)
    }
    isCancelled = true
  }

  override fun toString(): String {
    return "RetryFailedRecordsTask{recordQueue=$recordQueue, recordQueueSize=${recordQueue?.size}, isCancelled=$isCancelled}"
  }

  class Builder<O> {

    var recordQueue: BlockingQueue<ProducerRecord<String, O>>? = null
    var adminClient: KafkaAdminClient? = null
    var topicName: String? = null
    var producerHandle: KafkaProducer<String, O>? = null
    var dispatcherCallback: KafkaDispatcherCallback<O>? = null
    var correlationIdUpdater: EventCorrelationIdUpdater<O>? = null

    fun withRecordQueue(recordQueue: BlockingQueue<ProducerRecord<String, O>>?): Builder<O> {
      this.recordQueue = recordQueue
      return this
    }

    fun withAdminClient(adminClient: KafkaAdminClient): Builder<O> {
      this.adminClient = adminClient
      return this
    }

    fun withTopicName(topicName: String): Builder<O> {
      this.topicName = topicName
      return this
    }

    fun withProducerHandle(producerHandle: KafkaProducer<String, O>): Builder<O> {
      this.producerHandle = producerHandle
      return this
    }

    fun withDispatcherCallback(dispatcherCallback: KafkaDispatcherCallback<O>): Builder<O> {
      this.dispatcherCallback = dispatcherCallback
      return this
    }

    fun withEventCorrelationIdUpdater(
      correlationIdUpdater: EventCorrelationIdUpdater<O>?
    ): Builder<O> {
      this.correlationIdUpdater = correlationIdUpdater
      return this
    }

    fun build(): RetryFailedRecordsTask<O> {
      return RetryFailedRecordsTask(this)
    }
  }
}
