package fr.xebia.ldi.crocodile

import com.sksamuel.avro4s
import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.Config
import fr.xebia.ldi.crocodile.Configuration.CrocoConfig.{CrocoTask, CrocoTopic}
import fr.xebia.ldi.crocodile.schema.Click
import org.apache.avro.reflect.AvroSchema
import pureconfig.ConfigReader.Result
import pureconfig.generic.ProductHint
import pureconfig.{CamelCase, ConfigCursor, ConfigFieldMapping, ConfigReader, StringDelimitedNamingConvention}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, ManifestFactory}

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
  }

  case class CrocoConfig(kafkaConfig: Config, topics: Vector[CrocoTopic], taskConfig: CrocoTask)

  object CrocoConfig {

    case class CrocoSchema(subject: String, schema: AvroSchema)

    case class CrocoTask(schemaRegistryRetriesNum: Int,
                         schemaRegistryRetriesInterval: Duration,
                         topicCreationTimeout: Duration)

    case class CrocoTopic(name: String, partitions: Int, replicationFactor: Short)

  }

}
