package us.markwatson.kafka_reader

import us.markwatson.kafka_reader.consumer.Consumer

object KafkaReader {
  def main(args: Array[String]) = {
    println("Starting...")
    val c = new Consumer()
    c.run()
  }
}
