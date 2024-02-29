package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.TimeoutException
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.Queue
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import kotlin.streams.toList

/**
 * A sample simulation where a [KafkaProducer] encounters an error while sending a message.
 * We’ll assume that the message format is correct, but it fails to be published due to connectivity
 * issues or other transient errors.
 *
 * **Producer Flow:**
 * The Kafka Producer attempts to send a message to a specific topic.
 * If the message fails to be published (e.g., due to network issues or broker unavailability), the producer catches the exception.
 *
 * **DLQ Handling:**
 * The producer sends the failed message to a dedicated DLQ (LinkedBlockingDeque). The DLQ acts as a cache and persists across the application lifecycle.
 * It should have the same key and value as the original message (without any additional wrapping).
 * Additionally, include metadata in Kafka headers (e.g., original topic name, partition, offset, and timestamp).
 *
 * **Retrying from DLQ:**
 * The application periodically checks the DLQ for failed messages.
 * When the application determines that the connection is stable (e.g., network connectivity is restored), it retrieves messages from the DLQ.
 * The application reprocesses these messages by sending them back to the original topic.
 *
 * @author Aljan Porquillo
 */

private const val DEFAULT_TOPIC_LOOKUP_TIMEOUT_MS = 5000

private val deadLetterQueue: Queue<ProducerRecord<String?, String>> = LinkedBlockingDeque()

private fun <T>  Queue<T>.offerIfNotExist(data: T) = !contains(data) then offer(data)

fun main(args: Array<String>) {
  kafkaProducer(args)
}

@Throws(IOException::class)
fun loadProperties(fileName: String?): Properties {
  val envProps = Properties()
  val input = fileName?.let { FileInputStream(it) }
  envProps.load(input)
  input?.close()
  return envProps
}

private fun kafkaProducer(args: Array<String>) {
  check(args.size == 3) {
    log(
      """Arguments should contain the following:
         1. The config file for Kafka producer configs
         2. Text file containing the data to be sent to the topic
         3. The topic name
      """
    )
  }

  val props = loadProperties("config/dev.properties")
  val topic = args[2]

  KafkaProducer<String, String>(props).use { producer ->
    val linesToProduce = Files.readAllLines(Paths.get(args[1]))
    linesToProduce.stream().filter { l: String -> l.trim { it <= ' ' }.isNotEmpty() }
      .map { createRecord(it, topic) }
      .forEach { record ->
        try {
          trySend(
            producer, record, false
          ) { recordMetadata: RecordMetadata, e: Exception? ->
            if (e == null) {
              log(
                "Record written: ${record.value()} to offset" +
                    " ${recordMetadata.offset()} timestamp ${recordMetadata.timestamp()}"
              )
              deadLetterQueue.remove(record)
            } else {
              // potential cause for this exception would be kafka broker is not running
              if (e is TimeoutException) {
                log("${e.message} value: ${record.value()}")
                deadLetterQueue.offerIfNotExist(record)
              }
            }
          }
        } catch (ignored: InterruptedException) {
        } catch (ignored: ExecutionException) {
        }
      }

    // process remaining records in the DLQ
    val client = KafkaAdminClient.create(props)
    processRecordsFromDlq(client) { topics ->
      if (topics.isEmpty()) {
        return@processRecordsFromDlq
      }
      log("Kafka server is up and running with topics: $topics")
      while (deadLetterQueue.isNotEmpty()) {
        val record = deadLetterQueue.poll()
        trySend(producer, record, true) { metadata, e ->
          if (e == null) {
            log(
              "Successfully re-sent record with " +
                  "key=${record.key()} value=${record.value()} to topic"
            )
          }
        }
      }
    }
  }
}

fun createRecord(event: String, topic: String): ProducerRecord<String?, String> {
  val parts = event.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
  return if (parts.size > 1) {
    ProducerRecord(topic, parts[0], parts[1].trim())
  } else {
    ProducerRecord(topic, null, event)
  }
}

private fun processRecordsFromDlq(adminClient: AdminClient, onReady: (List<String>) -> Unit) {
  if (deadLetterQueue.isEmpty()) {
    log("Dead letter queue is empty. Nothing to re-send!")
    return
  }
  log(
    "There are at least ${deadLetterQueue.size} ${
      (deadLetterQueue.size > 1).then("records") ?: "record"} " +
        "in the DLQ. Will attempt to schedule sending all records to the topic once connected to Kafka",
  )
  adminClient.isKafkaAvailable(onReady = onReady)
}

private fun AdminClient.isKafkaAvailable(
  timeout: Int = DEFAULT_TOPIC_LOOKUP_TIMEOUT_MS,
  topicsOptions: ListTopicsOptions = ListTopicsOptions().timeoutMs(timeout),
  failFast: Boolean = false,
  onReady: (List<String>) -> Unit
) {
  while (true) {
    try {
      val kafkaFuture = listTopics(topicsOptions).listings()
      val collection = kafkaFuture.get()
      if (collection.isNotEmpty()) {
        val topics = collection.stream().map(TopicListing::name)
          .filter { name -> !name.startsWith("_") }
          .toList()
        onReady(topics)
        break
      }
    } catch (e: Exception) {
      when (e) {
        is ExecutionException,
        is InterruptedException -> {
          log("Kafka is not available, timed out after ms: $DEFAULT_TOPIC_LOOKUP_TIMEOUT_MS")
          if (failFast) {
            onReady(emptyList())
            break
          }
        }
      }
    }
  }
}

private fun trySend(
  producer: Producer<String, String>,
  record: ProducerRecord<String?, String>,
  log: Boolean,
  callback: Callback
): RecordMetadata? {
  if (log) {
    log("Attempting to re-send record with key=${record.key()} value=${record.value()} to topic")
  }
  return producer.send(record, callback).get()
}

private val PATTERN = DateTimeFormatter.ofPattern("HH:mm:ss")
private fun log(message: String) = println("${LocalTime.now().format(PATTERN)} $message")

infix fun <T> Boolean.then(param: T): T? = if (this) param else null