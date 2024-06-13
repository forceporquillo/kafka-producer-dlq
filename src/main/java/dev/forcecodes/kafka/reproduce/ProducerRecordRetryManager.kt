package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.protocol.types.Field.Str
import org.apache.logging.log4j.kotlin.logger
import java.util.*
import java.util.concurrent.*

fun String.toKafkaDaemonThreadFactory(): ThreadFactory {
  val topic = lowercase(Locale.getDefault()).replace("-topic", "")
  return ThreadFactory { runnable ->
    val thread = Executors.defaultThreadFactory().newThread(runnable)
    thread.setName("retry-$topic-thread")
    thread
  }
}

fun <O> Properties.newBlockingMechanism(
  capacity: String = getOrDefault("queueCapacity", Int.MAX_VALUE.toString()) as String
): BlockingQueue<O> = LinkedBlockingQueue(capacity.toInt())

fun ThreadFactory.singleThreadExecutor(): ExecutorService = Executors.newSingleThreadExecutor(this)

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
  properties: Properties,
  private val kafkaProducer: KafkaProducer<String, O>,
  private val topicName: String,
  private val adminClient: KafkaAdminClient = KafkaAdminClient.getInstance(properties),
  private val recordQueue: BlockingQueue<ProducerRecord<String, O>> = properties.newBlockingMechanism(),
  private val threadFactory: ThreadFactory = topicName.toKafkaDaemonThreadFactory(),
  private val retryTaskExecutor: ExecutorService = threadFactory.singleThreadExecutor()
): KafkaDispatcherCallback<O> {

  private val logger = logger()

  private var callback: KafkaDispatcherCallback<O>? = null
  private var hasOngoingRetry = false
  private var retryTaskFuture: CompletableFuture<Void>? = null

  private var internalDispatcherCallback: () -> Unit = {}

  override fun onDispatch(response: KafkaDispatcherResponse<O>) {
    when (response.state()) {
      KafkaDispatcherResponse.State.SUCCESS,
      KafkaDispatcherResponse.State.FAILURE,
      KafkaDispatcherResponse.State.RETRY -> {
        callback?.onDispatch(response)
      }
      KafkaDispatcherResponse.State.POLLED -> internalDispatcherCallback.invoke()
    }
    if (response.state() == KafkaDispatcherResponse.State.SUCCESS) {
      logger.debug(buildString {
        append("Record written: ${response.payload} to offset ")
        append("${response.metadata?.offset()} timestamp ${response.metadata?.timestamp()}")
      })
    }
  }

  fun setDispatchListener(dispatchListener: () -> Unit) {
    this.internalDispatcherCallback = dispatchListener
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
    logger.debug("Requesting correlation id update...")
    idUpdater.requestUpdate(record.value())
    val builder = KafkaDispatcherResponse.Builder<O>()
      .setKey(record.key())
      .setPayload(record.value())
      .setException(NoAvailableBrokersException())
      .setState(KafkaDispatcherResponse.State.RETRY)

    // dispatch with retry state
    callback?.onDispatch(builder.build())
    recordQueue.put(record)
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
      .withDispatcherCallback(this)
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