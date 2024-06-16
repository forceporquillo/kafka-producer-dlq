@file:Suppress("UNUSED")

package dev.forcecodes.kafka.reproduce

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.logging.log4j.kotlin.logger
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors

class KafkaAdminClient private constructor(
  config: Properties,
  private val timeout: Int,
  private val client: AdminClient = AdminClient.create(config),
  private val topicsOptions: ListTopicsOptions = ListTopicsOptions().timeoutMs(timeout)
) {

  private val logger = logger()

  fun isTopicsAvailable(logError: Boolean, listener: KafkaReadyListener) {
    while (true) {
      try {
        if (getAvailableTopics(logError, listener)) {
          break
        }
      } catch (e: Exception) {
        // cannot fetch topics, most likely Kafka broker is down
        if (logError) {
          logger.warn(String.format("Kafka broker not available, timeout after %sms", timeout))
        }
      }
    }
  }

  fun getAvailableTopics(logError: Boolean, listener: KafkaReadyListener): Boolean {
    return try {
      val collection: Collection<String> = client.listTopics(topicsOptions)
        .names()
        .get()
      if (!collection.isEmpty()) {
        val availableTopics = collection.stream()
          .filter { name: String -> !name.startsWith("_") }
          .collect(Collectors.toList())
        listener.onKafkaReady(availableTopics)
        return true
      }
      return false
    } catch (e: Exception) {
      // cannot fetch topics, most likely Kafka broker is down
      if (logError) {
        logger.warn(String.format("Kafka broker not available, timeout after %sms", timeout))
      }
      false
    }
  }

  companion object {

    private const val DEFAULT_TOPIC_LISTING_TIMEOUT_MS = 10000

    private val INSTANCE_MAP = ConcurrentHashMap<String, KafkaAdminClient>()

    private val Properties.listingTimeout: Int
      get() {
        val timeout = getOrDefault("listingTimeout", DEFAULT_TOPIC_LISTING_TIMEOUT_MS.toString()) as String
        return timeout.toInt()
      }

    @JvmStatic
    @Synchronized
    fun getInstance(config: Properties): KafkaAdminClient {
      return INSTANCE_MAP.computeIfAbsent(
        config.getProperty("bootstrap.servers")
      ) {
        KafkaAdminClient(
          config,
          config.listingTimeout
        )
      }
    }
  }
}