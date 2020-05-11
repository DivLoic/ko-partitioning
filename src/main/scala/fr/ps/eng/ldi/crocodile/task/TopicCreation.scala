package fr.ps.eng.ldi.crocodile.task

import java.util.concurrent.ExecutionException

import cats.syntax.either._
import fr.ps.eng.ldi.crocodile.Configuration.CrocoConfig
import fr.ps.eng.ldi.crocodile.Configuration.{CrocoConfig, _}
import org.apache.kafka.clients.admin.{Admin, CreateTopicsResult, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.auto._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}


/**
 * Created by loicmdivad.
 */
object TopicCreation extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  ConfigSource.default.load[CrocoConfig].map { config =>

    val client = Admin.create(config.kafkaConfig.toMap.asJava)

    val newTopics = config
      .application
      .topics
      .map { topic =>
        new NewTopic(topic.name, topic.partitions, topic.replicationFactor)
      }

    logger.info(s"Starting the topics creation for: ${config.application.topics.map(_.name).mkString(", ")}")

    val allKFutures: CreateTopicsResult = client.createTopics(newTopics.asJava)

    allKFutures.values().asScala.foreach { case (topicName, kFuture) =>

      kFuture.whenComplete {

        case (_, throwable: Throwable) if Option(throwable).isDefined =>
          logger warn("Topic creation didn't complete:", throwable)

        case _ =>
          newTopics.find(_.name() == topicName).map { topic =>
            logger.info(
              s"""|Topic ${topic.name}
                  | has been successfully created with ${topic.numPartitions} partitions
                  | and replicated ${topic.replicationFactor() - 1} times""".stripMargin.replaceAll("\n", "")
            )
          }
      }
    }

    val wait = config.taskConfig.topicCreationTimeout
    Try(allKFutures.all().get(wait._1, wait._2)) match {

      case Failure(ex) if ex.getCause.isInstanceOf[TopicExistsException] =>
        logger info "Topic creation stage completed. (Topics already created)"

      case failure@Failure(_: InterruptedException | _: ExecutionException) =>
        logger error "The topic creation failed to complete"
        failure.exception.printStackTrace()
        sys.exit(2)

      case Failure(exception) =>
        logger error "The following exception occurred during the topic creation"
        exception.printStackTrace()
        sys.exit(3)

      case Success(_) =>
        logger info "Topic creation stage completed."
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
