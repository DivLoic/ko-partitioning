package fr.ps.eng.ldi.crocodile

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import Configuration.CrocoConfig.{CrocoApp, CrocoTask, CrocoTopic}
import org.apache.avro.reflect.AvroSchema
import pureconfig.generic.ProductHint
import pureconfig.{CamelCase, ConfigFieldMapping, StringDelimitedNamingConvention}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/**
 * Created by loicmdivad.
 */
object Configuration {

  implicit def taskHint: ProductHint[CrocoTask] =
    ProductHint[CrocoTask](ConfigFieldMapping(CamelCase, new StringDelimitedNamingConvention(".")))

  implicit class configMapperOps(config: Config) {

    def toMap: Map[String, AnyRef] = config
      .entrySet()
      .asScala
      .map(pair => (pair.getKey, config.getAnyRef(pair.getKey)))
      .toMap

    def toProps: Properties = {
      val properties = new Properties()
      properties.putAll(config.toMap.asJava)
      properties
    }
  }

  implicit class propertiesOps(map: Map[String, AnyRef]) {
    def toProps: Properties = {
      val properties = new Properties()
      properties.putAll(map.asJava)
      properties
    }
  }

  case class CrocoConfig(application: CrocoApp,
                         kafkaConfig: Config = ConfigFactory.empty(),
                         taskConfig: CrocoTask = CrocoTask(300, Duration.Zero, Duration.Zero))

  object CrocoConfig {

    case class CrocoSchema(subject: String, schema: AvroSchema)

    case class CrocoTask(schemaRegistryRetriesNum: Int,
                         topicCreationTimeout: Duration,
                         schemaRegistryRetriesInterval: Duration)

    case class CrocoTopic(name: String, partitions: Int, replicationFactor: Short)

    case class CrocoApp(inputClickTopic: CrocoTopic, inputAccountTopic: CrocoTopic, outputResult: CrocoTopic) {

      def topics: Vector[CrocoTopic] = inputClickTopic :: inputAccountTopic :: outputResult :: Nil toVector
    }
  }

}
