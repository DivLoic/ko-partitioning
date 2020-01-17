package fr.xebia.ldi.crocodile.task

import java.io.IOException

import cats.syntax.either._
import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.ConfigFactory
import fr.xebia.ldi.crocodile.Configuration.CrocoConfig
import fr.xebia.ldi.crocodile.schema.{AccountId, Click}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures

import scala.util.{Failure, Success, Try}
import pureconfig.generic.auto._

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

/**
 * Created by loicmdivad.
 */
object SchemaCreation extends App {

  private val logger = LoggerFactory.getLogger(getClass)

  @tailrec
  def retryCallSchemaRegistry(countdown: Int, interval: Duration, f: => Unit): Try[Unit] = {
    Try(f) match {
      case result@Success(_) =>
        logger info "Successfully call the Schema Registry."
        result
      case result@Failure(_) if countdown <= 0 =>
        logger error "Fail to call the Schema Registry for the last time."
        result
      case Failure(_) if countdown > 0 =>
        logger error s"Fail to call the Schema Registry, retry in ${interval.toSeconds} secs."
        Thread.sleep(interval.toMillis)
        retryCallSchemaRegistry(countdown - 1, interval, f)
    }
  }

  val config = ConfigFactory.load
  ConfigSource.default.load[CrocoConfig].map { config =>

    val registryUrl = config.kafkaConfig.getString("schema.registry.url")
    val schemaRegistryClient = new CachedSchemaRegistryClient(registryUrl, 200)

    retryCallSchemaRegistry(
      config.taskConfig.schemaRegistryRetriesNum,
      config.taskConfig.schemaRegistryRetriesInterval, {
        schemaRegistryClient.register(s"CLICK-key", AvroSchema[AccountId])
        schemaRegistryClient.register(s"ACCOUNT-key", AvroSchema[AccountId])
        schemaRegistryClient.register(s"CLICK-value", AvroSchema[Click])
        schemaRegistryClient.register(s"ACCOUNT-value", AvroSchema[AccountId])
      }
    ) match {
      case failure@Failure(_: IOException | _: RestClientException) =>
        failure.exception.printStackTrace()
      case _ =>
        logger.info(String.format("Schemas publication at: %s", registryUrl))
    }

  }.recover {
    case failures: ConfigReaderFailures =>
      failures.toList.foreach(failure => logger.error(failure.description))
      sys.exit(1)

    case failures =>
      logger.error("Unknown error: ", failures)
      sys.exit(1)
  }
}
