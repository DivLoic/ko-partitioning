package fr.xebia.ldi.crocodile

import fr.xebia.ldi.crocodile.Configuration.CrocoConfig
import fr.xebia.ldi.crocodile.schema.{Account, AccountId, Click, UserEvent}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Joined, KStream, KTable, Produced}

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

    implicit val joinedClick: Joined[AccountId, Click, Account] = Joined.`with`

    implicit val producedClick: Produced[AccountId, UserEvent] = Produced.`with`

    val clickStreams: KStream[AccountId, Click] = builder.stream(config.application.inputClickTopic.name)

    val accountTable: KTable[AccountId, Account] = builder.table(config.application.inputAccountTopic.name)

    val newClicks: KStream[AccountId, UserEvent] = clickStreams.leftJoin(accountTable) { (click, maybeAccount) =>

      Option(maybeAccount).map(account => UserEvent(account, click)).orNull

    }

    newClicks.to(config.application.outputResult.name)

    builder.build
  }

}
