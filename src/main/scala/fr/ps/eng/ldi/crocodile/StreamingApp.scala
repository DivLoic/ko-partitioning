package fr.ps.eng.ldi.crocodile

import fr.ps.eng.ldi.crocodile.Configuration.{CrocoConfig, _}
import fr.ps.eng.ldi.crocodile.StreamingTopology.buildTopology
import org.apache.kafka.streams.KafkaStreams
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

    clickSerde :: accountSerde :: userEventSerde :: Nil foreach (_.configure(config.kafkaConfig.toMap.asJava, false))
    accountIdSerde.configure(config.kafkaConfig.toMap.asJava, true)

    val topology = buildTopology(config)()

    val streams = new KafkaStreams(topology, config.kafkaConfig.toProps)

    logger.info(topology.describe.toString)

    sys.addShutdownHook(streams.close())

    streams.start()
  }
}
