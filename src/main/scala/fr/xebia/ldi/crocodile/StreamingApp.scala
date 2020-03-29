package fr.xebia.ldi.crocodile

import fr.xebia.ldi.crocodile.Configuration.{CrocoConfig, _}
import fr.xebia.ldi.crocodile.schema.{Account, AccountId, Click, UserEvent}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Joined, KStream, KTable}
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource

import pureconfig.generic.auto._
import scala.jdk.CollectionConverters._

/**
 * Created by loicmdivad.
 */
object StreamingApp extends App with CrocoSerde {

  val logger = LoggerFactory.getLogger(getClass)

  ConfigSource.default.load[CrocoConfig].map { config =>

    clickSerde :: accountSerde :: userEventSerde :: Nil foreach(_.configure(config.kafkaConfig.toMap.asJava, false))
    accountIdSerde.configure(config.kafkaConfig.toMap.asJava, true)

    implicit val consumedClick: Consumed[AccountId, Click] =  Consumed.`with`(accountIdSerde, clickSerde)

    implicit val consumedAccount: Consumed[AccountId, Account] =  Consumed.`with`(accountIdSerde, accountSerde)

    implicit val joinedClick: Joined[AccountId, Click, Account] = Joined.`with`(accountIdSerde, clickSerde, accountSerde)

    implicit val producedClick: Produced[AccountId, UserEvent] = Produced.`with`(accountIdSerde, userEventSerde)

    val builder = new StreamsBuilder

    val clickStreams: KStream[AccountId, Click] = builder.stream(config.application.inputClickTopic)

    val accountTable: KTable[AccountId, Account] = builder.table(config.application.inputAccountTopic)

    val newClicks: KStream[AccountId, UserEvent] = clickStreams.leftJoin(accountTable) { (click, maybeAccount) =>

        Option(maybeAccount).map(account => UserEvent(account, click)).orNull

      }

    newClicks.to(config.application.outputResult)

    val streams = new KafkaStreams(builder.build, config.kafkaConfig.toProps)

    logger.info(builder.build.describe.toString)

    sys.addShutdownHook(streams.close())

    streams.start()
  }
}
