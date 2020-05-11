package fr.ps.eng.ldi.crocodile.task

import java.util.UUID

import cats.syntax.either._
import fr.ps.eng.ldi.crocodile.Configuration.{CrocoConfig, _}
import fr.ps.eng.ldi.crocodile.schema.Account.{AccountUpdate, Free, Gold, Plus}
import fr.ps.eng.ldi.crocodile.schema.{Account, AccountId, Click}
import fr.ps.eng.ldi.crocodile.{CrocoSerde, schemaNameMap}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.Cluster
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.auto._

import scala.jdk.CollectionConverters._

/**
 * Created by loicmdivad.
 */
object DataGeneration extends App with CrocoSerde {

  val logger = LoggerFactory.getLogger(getClass)

  ConfigSource.default.load[CrocoConfig].map { config =>

    clickSerde :: accountSerde :: Nil foreach (_.configure(config.kafkaConfig.toMap.asJava, false))
    accountIdSerde.configure(config.kafkaConfig.toMap.asJava, true)

    import scala.concurrent.duration._
    SchemaCreation.retryCallSchemaRegistry(logger)(10, 2 second, {
      val client = new CachedSchemaRegistryClient(config.kafkaConfig.getString("schema.registry.url"), 10)
      val subjects = client.getAllSubjects.asScala.toVector
      assert(subjects.length == schemaNameMap.size * 2)
    })

    val account1 = Account("matthieu@ps-eng.fr", Option(Plus), AccountUpdate())
    val account2 = Account("sylvain@ps-eng.fr", Option(Gold), AccountUpdate())
    val account3 = Account("sophie@ps-eng.fr", Option(Free), AccountUpdate())
    val account4 = Account("ben@ps-eng.fr", Option(Free), AccountUpdate())

    val accountProducer = new KafkaProducer[AccountId, Account](
      config.kafkaConfig.toMap.asJava,
      accountIdSerde.serializer(),
      accountSerde.serializer()
    )

    val clickProducer = new KafkaProducer[AccountId, Click](
      config.kafkaConfig.toMap.asJava,
      accountIdSerde.serializer(),
      clickSerde.serializer()
    )

    logger info "Sending the first Account events"

    (account1 :: account2 :: account3 :: account4 :: Nil zipWithIndex) foreach {
      case (account, index) =>
        val id = AccountId(s"ID-00$index")
        val record: ProducerRecord[AccountId, Account] = new ProducerRecord("ACCOUNT-TOPIC", id, account)
        accountProducer.send(record)
    }

    accountProducer.flush()

    logger info "Sending the clicks events"

    for (_ <- 0 to 100) {

      for (index <- 0 to 3) {
        Thread.sleep(1000)
        val id = AccountId(s"ID-00$index")
        val click = Click(UUID.randomUUID().toString)
        val record = new ProducerRecord("CLICK-TOPIC", id, click)
        clickProducer.send(record)
      }
    }

    logger info "Closing the data generator ..."
  }

    .recover {
      case failures: ConfigReaderFailures =>
        failures.toList.foreach(failure => logger.error(failure.description))
        sys.exit(1)

      case failures =>
        logger.error("Unknown error: ", failures)
        sys.exit(1)
    }

  object CustomPartitioner {

    class ConstPartitioner extends DefaultPartitioner {
      override def partition(a: String, b: Any, c: Array[Byte], d: Any, e: Array[Byte], f: Cluster): Int = 2
    }
  }
}
