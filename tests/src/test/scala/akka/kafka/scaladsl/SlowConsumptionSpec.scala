/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{Actor, ActorRef}
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.test.Utils._
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import org.scalatest._
import akka.kafka.KafkaConsumerActor

import scala.concurrent.Future
import scala.concurrent.duration._

class SlowConsumptionSpec extends SpecBase(kafkaPort = KafkaPorts.SlowConsumptionSpec) with Inside {

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort,
                        zooKeeperPort,
                        Map(
                          "offsets.topic.replication.factor" -> "1"
                        ))

  /* This test case exercises a slow consumer which leads to a behaviour where the Kafka client
   * discards already fetched records, which is shown by the `o.a.k.c.consumer.internals.Fetcher`
   * log message
   * "Not returning fetched records for assigned partition topic-1-1-0 since it is no longer fetchable".
   *
   * Add <logger name="org.apache.kafka.clients.consumer.internals.Fetcher" level="DEBUG"/>
   * to tests/src/test/resources/logback-test.xml to see it.
   *
   * It was implemented to understand the unwanted behaviour reported in issue #549
   * https://github.com/akka/alpakka-kafka/issues/549
   *
   * I did not find a way to inspect this discarding of fetched records from the outside, yet.
   */

  val producerSettings = ProducerSettings(system, new StringSerializer, new IntegerSerializer)
    .withBootstrapServers(bootstrapServers)

  def producer(topic: String, partition: Int): (UniqueKillSwitch, Future[Integer]) =
    Source
      .fromIterator(() => Iterator.from(2))
      .viaMat(KillSwitches.single)(Keep.right)
      .map(
        n => ProducerMessage.Message[String, Integer, Integer](new ProducerRecord(topic, partition, DefaultKey, n), n)
      )
      .via(Producer.flexiFlow(producerSettings))
      .map(_.passThrough)
      .toMat(Sink.last)(Keep.both)
      .run()

  def consumer(consumerActor: ActorRef,
               topic: String,
               partition: Int,
               rate: FiniteDuration): Consumer.DrainingControl[Integer] =
    Consumer
      .plainExternalSource[String, Integer](consumerActor,
                                            Subscriptions.assignment(new TopicPartition(topic, partition)))
      .throttle(1, rate)
      .map(record => {
        val received = record.value()
        log.info(s"Consumer of topic $topic received: $received")
        received
      })
      .toMat(Sink.last)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()

  "Reading slower than writing" must {
    "work" in assertAllStagesStopped {

      val groupId = createGroupId(1)
      val topic1 = createTopic(1)
      val topic2 = createTopic(2)

      val (producer1Switch, producer1LastProduced) = producer(topic1, partition0)
      val (producer2Switch, producer2LastProduced) = producer(topic2, partition0)

      val sharedConsumerActor = system.actorOf(
        KafkaConsumerActor.props(
          ConsumerSettings(system, new StringDeserializer, new IntegerDeserializer)
            .withBootstrapServers(bootstrapServers)
            .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withWakeupTimeout(10.seconds)
            .withMaxWakeups(10)
            .withGroupId(groupId)
            .withProperty("max.poll.records", "5")
        )
      )

      val consumer1control = consumer(sharedConsumerActor, topic1, partition0, 1.second)
      val consumer2control = consumer(sharedConsumerActor, topic2, partition0, 50.millisecond)

      sleep(1.minute)
      producer1Switch.shutdown()
      producer2Switch.shutdown()

      val consumer1LastConsumed = consumer1control.drainAndShutdown()
      val consumer2LastConsumed = consumer2control.drainAndShutdown()

      val (produced1, consumed1, produced2, consumed2) = Future
        .sequence(Seq(producer1LastProduced, consumer1LastConsumed, producer2LastProduced, consumer2LastConsumed))
        .map {
          case Seq(p1, c1, p2, c2) =>
            println(s"P1: last element produced $p1")
            println(s"C1: last element consumed $c1")
            println(s"P2: last element produced $p2")
            println(s"C2: last element consumed $c2")
            (p1, c1, p2, c2)
        }
        .futureValue

      import akka.kafka.KafkaConsumerActor
      sharedConsumerActor.tell(KafkaConsumerActor.stop, Actor.noSender)

      produced1 should be > consumed1
      produced2 should be > consumed2

    }
  }
}
