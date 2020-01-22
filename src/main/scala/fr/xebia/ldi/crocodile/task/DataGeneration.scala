package fr.xebia.ldi.crocodile.task

import java.util.UUID

import fr.xebia.ldi.crocodile.Configuration.CrocoConfig
import fr.xebia.ldi.crocodile.CrocoSerde
import fr.xebia.ldi.crocodile.schema.Account.{AccountUpdate, Free}
import fr.xebia.ldi.crocodile.schema.{Account, AccountId, Click}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import fr.xebia.ldi.crocodile.Configuration._
import scala.jdk.CollectionConverters._
import cats.syntax.either._
import pureconfig.generic.auto._

/**
 * Created by loicmdivad.
 */
object DataGeneration extends App with CrocoSerde {

  val logger =  LoggerFactory.getLogger(getClass)

  ConfigSource.default.load[CrocoConfig]

    .map { config =>

      clickSerde :: accountSerde :: Nil foreach(_.configure(config.kafkaConfig.toMap.asJava, false))
      accountIdSerde.configure(config.kafkaConfig.toMap.asJava, true)

      val account1 = Account("matthieu@xebia.fr", Free, Option(AccountUpdate()))
      val account2 = Account("sylvain@xebia.fr", Free, Option(AccountUpdate()))
      val account3 = Account("sophie@xebia.fr", Free, Option(AccountUpdate()))
      val account4 = Account("ben@xebia.fr", Free, Option(AccountUpdate()))

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

      (account1 :: account2 :: account3 :: account4 :: Nil zipWithIndex) foreach {
        case (account, index) =>
            val id = AccountId(s"ID-00$index")
            val record: ProducerRecord[AccountId, Account] = new ProducerRecord("ACCOUNT-TOPIC", id, account)
          accountProducer.send(record)
      }

      for(_ <- 0 to 100) {

        for (index <- 1 to 4) {
          Thread.sleep(1000)
          val id = AccountId(s"ID-00$index")
          val click = Click(UUID.randomUUID().toString)
          val record = new ProducerRecord("CLICK-TOPIC", id, click)
          clickProducer.send(record)
        }
      }
    }

    .recover {
      case failures: ConfigReaderFailures =>
        failures.toList.foreach(failure => logger.error(failure.description))
        sys.exit(1)

      case failures =>
        logger.error("Unknown error: ", failures)
        sys.exit(1)
    }

}
