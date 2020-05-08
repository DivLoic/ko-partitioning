package fr.xebia.ldi.crocodile.task.consumer

import fr.xebia.ldi.crocodile.Configuration.{CrocoConfig, _}
import fr.xebia.ldi.crocodile.schema.{AccountId, UserEvent}
import fr.xebia.ldi.crocodile.{ColorizedConsumer, CrocoSerde}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.jdk.CollectionConverters._

/**
 * Created by loicmdivad.
 */
object ClickStream extends App with ColorizedConsumer with CrocoSerde  {

  val logger = LoggerFactory.getLogger(getClass)

  ConfigSource.default.load[CrocoConfig].map { config =>

    val streamConfig = config.kafkaConfig.toMap + ((StreamsConfig.APPLICATION_ID_CONFIG, s"CLICK-CONSUMER"))

    implicit val consumedUserEvent: Consumed[AccountId, UserEvent] =
      Consumed.`with`(accountIdSerde, userEventSerde).withName("user-event-consumer")

    userEventSerde.configure(streamConfig.asJava, false)
    accountIdSerde.configure(streamConfig.asJava, true)

    val builder = new StreamsBuilder

    builder

      .stream(config.application.outputResult)

      .selectKey((account, _) => account.copy(colorize(account.value)))

      .print(Printed.toSysOut[AccountId, UserEvent].withLabel("📱CLICKS"))


    val streams = new KafkaStreams(builder.build, streamConfig.toProps)

    logger.info(builder.build.describe.toString)

    sys.addShutdownHook(streams.close())

    streams.start()

  }

}
