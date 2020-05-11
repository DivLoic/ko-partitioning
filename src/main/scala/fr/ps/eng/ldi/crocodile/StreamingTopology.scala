package fr.ps.eng.ldi.crocodile

import fr.ps.eng.ldi.crocodile.Configuration.CrocoConfig
import fr.ps.eng.ldi.crocodile.schema.{Account, AccountId, Click, UserEvent}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, Serdes, StreamsBuilder}

/**
 * Created by loicmdivad.
 */
object StreamingTopology {

  def buildTopology(config: CrocoConfig)
                   (builder: StreamsBuilder = new StreamsBuilder)
                   (implicit
                    s1: Serde[Click],
                    s2: Serde[Account],
                    s3: Serde[AccountId],
                    s4: Serde[UserEvent]): Topology = {

    implicit val stringSerde: Serde[String] = Serdes.String

    implicit val consumedClick: Consumed[String, Click] =  Consumed.`with`

    implicit val consumedAccount: Consumed[AccountId, Account] =  Consumed.`with`

    implicit val materializedAccount: Materialized[String, Account, ByteArrayKeyValueStore] = Materialized.`with`

    implicit val groupedAccount: Grouped[String, Account] = Grouped.`with`

    implicit val joinedClick: Joined[String, Click, Account] = Joined.`with`

    implicit val producedClick: Produced[String, UserEvent] = Produced.`with`

    val clickStreams: KStream[String, Click] = builder.stream(config.application.inputClickTopic.name)

    val accountTable: KTable[String, Account] = builder

      .stream(config.application.inputAccountTopic.name)(consumedAccount)

      .selectKey((key, _) => key.value)

      .toTable(materializedAccount)

    val newClicks: KStream[String, UserEvent] = clickStreams.leftJoin(accountTable){ (click, maybeAccount) =>

        Option(maybeAccount).map(account => UserEvent(account, click)).orNull

      }

    newClicks.to(config.application.outputResult.name)

    builder.build
  }
}