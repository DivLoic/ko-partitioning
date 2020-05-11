package fr.ps.eng.ldi.crocodile

import fr.ps.eng.ldi.crocodile.Configuration.CrocoConfig
import fr.ps.eng.ldi.crocodile.schema.{Account, AccountId, Click, UserEvent}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}

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

    implicit val consumedClick: Consumed[AccountId, Click] =  Consumed.`with`

    implicit val consumedAccount: Consumed[AccountId, Account] =  Consumed.`with`

    implicit val materializedAccount: Materialized[AccountId, Account, ByteArrayKeyValueStore] = Materialized.`with`

    implicit val joinedClick: Joined[AccountId, Click, Account] = Joined.`with`

    implicit val producedClick: Produced[AccountId, UserEvent] = Produced.`with`

    val clickStreams: KStream[AccountId, Click] = builder.stream(config.application.inputClickTopic.name)

    val accountTable: GlobalKTable[AccountId, Account] = builder
      .globalTable(config.application.inputAccountTopic.name, materializedAccount)

    val newClicks: KStream[AccountId, UserEvent] = clickStreams.leftJoin(accountTable)(
      (id, _) => id,
      (click, maybeAccount) => Option(maybeAccount).map(account => UserEvent(account, click)).orNull
    )

    newClicks.to(config.application.outputResult.name)

    builder.build
  }

}