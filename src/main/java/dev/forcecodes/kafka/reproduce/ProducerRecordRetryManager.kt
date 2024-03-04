package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TimeoutException
import org.apache.logging.log4j.kotlin.logger
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory

/**
 * This class manages the retry logic for a
 * [KafkaProducer][org.apache.kafka.clients.producer.KafkaProducer] when sending a
 * [ProducerRecord] to a given topic fails. It operates in the background, attempting to
 * resend failed records and handling callbacks to the caller. Failed records are queued for
 * resending once the Kafka broker becomes available.
 *
 * @param [O] The type of object to dispatch to a callback listener.
 * @author Aljan Porquillo
 */
class ProducerRecordRetryManager<O>(
  properties: Properties?,
  kafkaProducer: KafkaProducer<String, O>,
  topicName: String
) {

  private val logger = logger()

  private val adminClient: KafkaAdminClient
  private val kafkaProducer: KafkaProducer<String, O>
  private val topicName: String
  private val recordQueue: BlockingQueue<ProducerRecord<String, O>>
  private val retryTaskExecutor: ExecutorService
  private var callback: KafkaDispatcherCallback<O>? = null
  private var hasOngoingRetry = false
  private var retryTaskFuture: CompletableFuture<Void>? = null

  init {
    this.adminClient = KafkaAdminClient.getInstance(properties!!)
    this.kafkaProducer = kafkaProducer
    this.topicName = topicName
    this.recordQueue = LinkedBlockingQueue()
    this.retryTaskExecutor = Executors.newSingleThreadExecutor(createHandlerThreadFactory())
  }

  private fun createHandlerThreadFactory(): ThreadFactory {
    val topic = topicName.lowercase(Locale.getDefault()).replace("-topic", "")
    return ThreadFactory { runnable ->
      val thread = Executors.defaultThreadFactory().newThread(runnable)
      thread.setName("retry-$topic-thread")
      thread
    }
  }

  fun setCallback(callback: KafkaDispatcherCallback<O>) {
    this.callback = callback
  }

  fun queueFailedRecord(
    exception: Exception,
    record: ProducerRecord<String, O>,
    idUpdater: EventCorrelationIdUpdater<O>
  ) {
    if (exception !is TimeoutException) {
      return
    }
    enqueueToBucket(record, idUpdater)
    ensureRetryTaskIsRunning(idUpdater)
  }

  private fun enqueueToBucket(
    record: ProducerRecord<String, O>,
    idUpdater: EventCorrelationIdUpdater<O>
  ) {
    logger.debug("Putting $record in the queue for retry once the Kafka broker becomes available")

    // request to update eventId for current record since
    // we will be pushing new event to the Audit report.
    idUpdater.requestUpdate(record.value())
    val builder = KafkaDispatcherResponse.Builder<O>()
      .setKey(record.key())
      .setPayload(record.value())

    // dispatch with retry state
    callback?.onDispatch(builder.setState(KafkaDispatcherResponse.State.RETRY).build())
    recordQueue.add(record)
  }

  private fun ensureRetryTaskIsRunning(idUpdater: EventCorrelationIdUpdater<O>): CompletableFuture<Void> {
    logger.debug("RetryFailedRecordsTask state: ${if (hasOngoingRetry) "RUNNING" else "IDLE"}")
    if (hasOngoingRetry) {
      return retryTaskFuture.getOrComplete()
    }
    logger.debug("RetryFailedRecordsTask state: STARTING")
    hasOngoingRetry = true
    val retryRecordTask: Runnable = createRetryTask()
      .withEventCorrelationIdUpdater(idUpdater)
      .build()
    retryTaskFuture = executeRetryFailedRecordsTask(retryRecordTask)
    return retryTaskFuture.getOrComplete()
  }

  private fun executeRetryFailedRecordsTask(retryRecordTask: Runnable): CompletableFuture<Void> {
    // execute the task asynchronously to avoid blocking the current producer handle
    logger.debug("$retryRecordTask started")
    return CompletableFuture.runAsync(retryRecordTask, retryTaskExecutor)
      .thenRun {
        logger.debug("$retryRecordTask state: COMPLETED. Resetting to the initial retry state: IDLE")
        hasOngoingRetry = false
      }
  }

  private fun createRetryTask(): RetryFailedRecordsTask.Builder<O> {
    return RetryFailedRecordsTask.Builder<O>()
      .withProducerHandle(kafkaProducer)
      .withAdminClient(adminClient)
      .withRecordQueue(recordQueue)
      .withTopicName(topicName)
      .withDispatcherCallback { response ->
        callback?.onDispatch(response)
        if (response.state() == KafkaDispatcherResponse.State.SUCCESS) {
          logger.debug("Record written: ${response.payload} to offset ${response.metadata?.offset()} timestamp ${response.metadata?.timestamp()}")
        }
      }
  }

  fun tryNotifyPendingRecords(record: ProducerRecord<String, O>, idUpdater: EventCorrelationIdUpdater<O>): Boolean {
    if (hasOngoingRetry) {
      enqueueToBucket(record, idUpdater)
      logger.debug("Notifying pending records to resend from a topic: $topicName, " +
          "record old size: ${recordQueue.size - 1}, new size ${recordQueue.size}")
      return false
    }
    return true
  }

  private fun CompletableFuture<Void>?.getOrComplete(): CompletableFuture<Void> {
    return this ?: CompletableFuture.completedFuture(null)
  }
}