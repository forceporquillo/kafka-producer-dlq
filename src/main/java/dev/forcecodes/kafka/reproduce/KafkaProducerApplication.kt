package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingDeque
import java.util.function.Consumer
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
 * The producer sends the failed message to a dedicated DLQ topic.
 * The DLQ topic should have the same key and value as the original message (no additional wrapping).
 * Additionally, include metadata in Kafka headers (e.g., original topic name, partition, offset, timestamp, and error details).
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
              log("${e.message} value: ${record.value()}")
              if (!deadLetterQueue.contains(record)) {
                deadLetterQueue.offer(record)
              }
            }
          }
        } catch (ignored: InterruptedException) {
        } catch (ignored: ExecutionException) {
        }
      }

    processRecordsFromDlq { topics ->
      if (topics.isEmpty()) {
        return@processRecordsFromDlq
      }
      log("Kafka server is up and running with topics: $topics")
      while (deadLetterQueue.isNotEmpty()) {
        val record = deadLetterQueue.poll()
        trySend(producer, record, true) { _, e ->
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
  val parts = event.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
  return if (parts.size > 1) {
    ProducerRecord(topic, parts[0], parts[1].trim())
  } else {
    ProducerRecord(topic, null, parts[0])
  }
}

private fun processRecordsFromDlq(onReady: Consumer<List<String>>) {
  if (deadLetterQueue.isEmpty()) {
    log("Dead letter queue is empty. Nothing to re-send!")
    return
  }

  val properties = Properties()
  properties["bootstrap.servers"] = "localhost:29092"

  val client = AdminClient.create(properties)
  val options = ListTopicsOptions().timeoutMs(DEFAULT_TOPIC_LOOKUP_TIMEOUT_MS)

  while (true) {
    try {
      val kafkaFuture = client.listTopics(options).listings()
      val collection = kafkaFuture.get()
      if (collection.isNotEmpty()) {
        val topics = collection.stream().map(TopicListing::name)
          .filter { name -> !name.startsWith("_") }.toList()
        onReady.accept(topics)
        break
      }
    } catch (e: Exception) {
      when (e) {
        is ExecutionException, is InterruptedException -> {
          log("Kafka is not available, timed out after ms: $DEFAULT_TOPIC_LOOKUP_TIMEOUT_MS")
          onReady.accept(emptyList())
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