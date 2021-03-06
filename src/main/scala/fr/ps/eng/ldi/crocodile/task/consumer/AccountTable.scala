package fr.ps.eng.ldi.crocodile.task.consumer

import java.time.format.DateTimeFormatter
import java.util.UUID

import fr.ps.eng.ldi.crocodile.Configuration._
import fr.ps.eng.ldi.crocodile.schema.{Account, AccountId}
import fr.ps.eng.ldi.crocodile.{ColorizedConsumer, CrocoSerde}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.streams.kstream.{Produced, ValueTransformerWithKey}
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.scala.kstream.{Consumed, KTable, Materialized}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.state.{ReadOnlyKeyValueStore, ValueAndTimestamp}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import pureconfig.generic.auto._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.reflect.io.File
import scala.util.Try

/**
 * Created by loicmdivad.
 */
object AccountTable extends App with ColorizedConsumer with CrocoSerde {

  val logger = LoggerFactory.getLogger(getClass)
  val tmpTableFile: File = File(s"/tmp/${UUID.randomUUID().toString}.txt")

  def accountTimeFormat(account: Account) = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    .withZone(account.lastUpdate.zoneId)

  val InternalAccountTopic = "ACCOUNT-TOPIC-CONSUMER-REPARTITION"

  import AccountConsumerDisplay._

  ConfigSource.default.load[CrocoConfig].map { config =>

    Try(Admin
      .create(config.kafkaConfig.toMap.asJava)
      .createTopics(new NewTopic(InternalAccountTopic, 1, 1 toShort) :: Nil asJava).all().get())

    val streamConfig = config.kafkaConfig.toMap + ((StreamsConfig.APPLICATION_ID_CONFIG, s"ACCOUNT-CONSUMER"))

    accountSerde.configure(streamConfig.asJava, false)
    accountIdSerde.configure(streamConfig.asJava, true)

    implicit val consumedAccount: Consumed[AccountId, Account] =
      Consumed.`with`(accountIdSerde, accountSerde)

    implicit val producedAccount: Produced[AccountId, Account] =
      Produced.`with`(accountIdSerde, accountSerde)

    implicit val materializedAccount: Materialized[AccountId, Account, ByteArrayKeyValueStore] =
      Materialized.as("account-table")(accountIdSerde, accountSerde)

    val builder = new StreamsBuilder

    val accountTable: KTable[AccountId, Account] = builder

      .stream(config.application.inputAccountTopic.name)

      .through(InternalAccountTopic)

      .toTable(materializedAccount)

    accountTable
      .transformValues(() => new ValueTransformerWithKey[AccountId, Account, Unused] {
          private var context: ProcessorContext = _
          private var store: ReadOnlyKeyValueStore[AccountId, ValueAndTimestamp[Account]] = _

          override def close(): Unit = {}

          override def transform(readOnlyKey: AccountId, value: Account): Unused = Option.empty

          override def init(context: ProcessorContext): Unit = {
            import scala.concurrent.duration._
            this.context = context
            this.store = this
              .context
              .getStateStore("account-table")
              .asInstanceOf[ReadOnlyKeyValueStore[AccountId, ValueAndTimestamp[Account]]]

            this.context.schedule(2.seconds toJava, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
              override def punctuate(timestamp: Long): Unit = {
                val header: String = TableLineBorder
                val lines: List[String] = store.all().asScala.toList.flatMap(kv => makeLine(kv.key, kv.value))

                tmpTableFile.writeAll("\n" :: (header :: lines) map(_ + "\n") map colorize:_*)

              }
            })
          }
        }, "account-table")

    val streams = new KafkaStreams(builder.build, streamConfig.toProps)

    logger.info(builder.build.describe.toString)

    sys.addShutdownHook {
      tmpTableFile.delete()
      streams.close()
    }

    streams.start()

    import sys.process._
    s"""watch --color -t -n 1 "cat ${tmpTableFile.path}" """ !

  }

  object AccountConsumerDisplay {

    type Unused = Option[Nothing]

    val TableLineBorder: String = "+ ".padTo(9, "-").mkString +
        " + ".padTo(20, "-").mkString +
        " + ".padTo(10, "-").mkString +
        " + ".padTo(30, "-").mkString + " +"

    def makeLine(accountId: AccountId, tsAccount: ValueAndTimestamp[Account]): List[String] = {
      val account = tsAccount.value()

      s"| ${accountId.value}".padTo(10, " ").mkString +
        s"| ${account.login}".padTo(20, " ").mkString +
        s"| ${account.plan.getOrElse(Account.None)}".padTo(10, " ").mkString +
        s"| ${account.lastUpdate.datetime.format(accountTimeFormat(account))}".padTo(30, " ").mkString + "|" ::
        TableLineBorder :: Nil
    }
  }
}
