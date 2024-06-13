package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.logging.log4j.kotlin.logger
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ExecutionException
import kotlin.system.exitProcess

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
object KafkaProducerApplication {

  private val logger = logger()

  @Throws(IOException::class)
  fun loadProperties(fileName: String?): Properties {
    val envProps = Properties()
    val input = fileName?.let { FileInputStream(it) }
    envProps.load(input)
    input?.close()
    return envProps
  }

  private fun KafkaProducer<String, RecordData>.trySend(
    record: ProducerRecord<String, RecordData>,
    onError: (Exception) -> Unit
  ) {
    try {
      send(record) { recordMetadata, exception ->
        if (exception == null) {
          logger.debug("Record written: ${record.value()} to offset ${recordMetadata.offset()} timestamp ${recordMetadata.timestamp()}")
        } else {
          onError.invoke(exception)
        }
      }.get()
    } catch (e: ExecutionException) {
      e.message?.let { logger.error(it) }
    }
  }

  private fun kafkaProducer(args: Array<String>) {
    check(args.size == 3) {
      logger.warn(
        """Arguments should contain the following:
         1. The config file for Kafka producer configs
         2. Text file containing the data to be sent to the topic
         3. The topic name
      """
      )
    }

    val props = loadProperties(args[0])
    val topic = args[2]

    val producer = KafkaProducer<String, RecordData>(props)
    val retryManager = ProducerRecordRetryManager(props, producer, topic)

    logger.debug("properties: $props")
    logger.debug("topic: $topic")

    retryManager.setDispatchListener {
      logger.debug("All records has been successfully re-send to topic, closing program...")
      exitProcess(1)
    }

    retryManager.setCallback {
      logger.debug("Callback: $it")
    }

    val linesToProduce = Files.readAllLines(Paths.get(args[1]))
    linesToProduce.stream().filter { l: String -> l.trim { it <= ' ' }.isNotEmpty() }
      .map { it.toRecord(topic) }
      .forEach { record ->
        logger.debug("Record to send $record")

        val proceedToSend = retryManager.tryNotifyPendingRecords(record) {
          onUpdateCorrelationId(record.value(), "NotifyPendingRecords")
        }

        if (proceedToSend) {
          producer.trySend(record) { exception ->
            // potential cause for this exception would be kafka broker is not running
            retryManager.queueFailedRecord(exception, record) {
              onUpdateCorrelationId(record.value(), "FailedRecords")
            }
          }
        }
      }
  }

  private fun onUpdateCorrelationId(recordData: RecordData, callSite: String) {
    logger.debug("Old correlation id ${recordData.correlationId}")
    val correlationId = UUID.randomUUID().toString()
    recordData.correlationId = correlationId
    logger.debug("New correlation id $correlationId")
    logger.debug("$callSite: Consuming correlation updater callback : $recordData with update message $correlationId")
  }

  private fun String.toRecord(topic: String): ProducerRecord<String, RecordData> {
    val parts = split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
    val id = UUID.randomUUID().toString()
    return if (parts.size > 1) {
      ProducerRecord(topic, parts[0], RecordData(parts[1].trim(), correlationId = id))
    } else {
      ProducerRecord(topic, id, RecordData(this, correlationId = id))
    }
  }

  @JvmStatic
  fun main(args: Array<String>) {
    kafkaProducer(args)
  }
}