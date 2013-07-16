package us.markwatson.kafka_reader.consumer

import java.util.Properties
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.message.Message
import com.google.common.collect.ImmutableMap
import java.util
import scala.collection.JavaConversions._

class Consumer() {
  def run() {
    val topicName: String = "test"
    val numWorkers: Int = 4

    val props = new Properties()

    props.put("zk.connect", "localhost:2181")
    props.put("zk.connectiontimeout.ms", "1000000")
    props.put("groupid", "test")
    props.put("fromBeginning", "true")

    // Create the connection to the cluster
    val consumerConfig = new ConsumerConfig(props)
    val consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig)

    val topicMessageStreams: util.Map[String, util.List[KafkaStream[Message]]] =
      consumerConnector.createMessageStreams(ImmutableMap.of(topicName, numWorkers))
    val streams: util.List[KafkaStream[Message]] = topicMessageStreams.get(topicName)

    for(stream <- streams) {
      new Thread(new Runnable {
        def run() {
          val coord = new SimpleMessageCoordinator(new BinaryMessageHandler)
          for (message <- stream) {
            println(coord.handle(message.message.payload))
          }
        }
      }).start()
    }
  }
}
