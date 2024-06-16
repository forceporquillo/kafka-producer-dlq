# Kafka Producer with Dead Letter Queue (Cache)

---

A sample simulation where a KafkaProducer encounters an error while sending a message. We’ll assume
that the message format is correct, but it fails to be published due to connectivity issues or other
transient errors.

**Producer Flow:** The Kafka Producer attempts to send a message to a specific topic. If the message
fails to be published (e.g., due to network issues or broker unavailability), the producer catches
the exception.

**Handling Failed Records:** The producer sends the failed message to a dedicated DLQ (Dead Letter Queue) - like (LinkedBlockingDeque). The
DLQ acts as an ephemeral cache and persists across the application lifecycle. It should have the same key and
value as the original message (without any additional wrapping). Additionally, include metadata in
Kafka headers (e.g., original topic name, partition, offset, and timestamp).

**Retrying from DLQ:** The application periodically checks the DLQ for failed messages (pre-configured ~10 seconds). When the
application determines that the connection is stable (e.g., network connectivity is restored), it
retrieves messages from the DLQ. The application reprocesses these messages by sending them back to
the original topic.

## Prerequisites
Before you begin, ensure you have the following prerequisites:

- **Java 11** or higher
- **Docker** installed
- **Bash** (optional) - required for running the `./bootstrap.sh` script

## Kafka Producer Configuration
The minimum configuration for the Kafka producer is specified in the [config/dev.properties](config/dev.properties) file. Below are the key settings:

1. **`bootstrap.servers`**: This property defines the list of Kafka brokers to which the producer connects. In this case, it points to `localhost:29092`, assuming that we run our Kafka broker locally on port `29092`.

2. **`max.block.ms`**: The `max.block.ms` setting determines the maximum time (in milliseconds) that the producer will wait for a response from the Kafka broker during certain operations. By reducing this timeout to 500 milliseconds, we can simulate faster interactions.

3. **`key.serializer`** and **`value.serializer`**: These properties specify the serializers for the keys and values of the messages produced by the Kafka producer. In this example, we're using the `StringSerializer` for both keys and values.

```properties
# Kafka Producer Configuration
bootstrap.servers=localhost:29092

# reduce max.block timeout so we can simulate faster
max.block.ms=500

key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=dev.forcecodes.kafka.reproduce.RecordDataSerializer
```
For more comprehensive details and additional configuration options, I recommend referring to the official [Kafka](https://kafka.apache.org/documentation/) documentation.

---

### Application Configuration
There’s also an optional configuration for the application that can be added in [config/dev.properties](config/dev.properties). Below are the following configuration parameters:
1. `queueCapacity`: The total capacity to queue failed records at a time. Eventually blocks the queue if it's already full. Default: `Int.MAX_VALUE`
2. `listingTimeout`: The poll timeout for checking for available topic(s) in a Kafka broker. Default `10000ms`

```properties
# Kafka Producer Configuration
...

# Application Configuration
queueCapacity=100
listingTimeout=2000
```

## Building from Source

1. Start Docker Compose: Run `docker-compose up -d` (Ensure that Docker/Docker Desktop is currently running on your machine).

2. Create a Kafka topic:
   ```
   /usr/bin/kafka-topics --create --topic test-out-topic --bootstrap-server broker:29092 --partitions 1
   ```
   Alternatively, if you prefer creating a topic via terminal or CLI, append `docker-compose exec broker` before the command above, and make sure you are within the project directory.

3. The project is written in Kotlin. Prioritize building Kotlin, followed by a regular Maven build:
   ```
   kotlin:compile install -f pom.xml
   ```

4. Once the build is complete, execute the script:
   ```
   ./bootstrap.sh config/dev.properties sample_data.txt test-out-topic
   ```
   Note that `bootstrap.sh` requires three arguments provided in the following order:
    - The configuration file for Kafka producer settings. (`config/dev.properties`)
    - A text file containing the data to be sent to the topic. (`sample_data.txt`)
    - The topic name. (`test-out-topic`)

# License

```
Designed and developed by 2024 strongforce1 (Aljan Porquillo)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

