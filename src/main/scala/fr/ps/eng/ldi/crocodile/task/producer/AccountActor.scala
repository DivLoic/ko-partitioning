package fr.ps.eng.ldi.crocodile.task.producer

import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.kafka.ProducerMessage.Message
import fr.ps.eng.ldi.crocodile.schema.Account.{AccountUpdate, Free, Gold, Plus}
import fr.ps.eng.ldi.crocodile.schema.{Account, AccountId}
import fr.ps.eng.ldi.crocodile.task.producer.AccountActor.{Start, Stop, Update}
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalacheck.Gen
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.{FiniteDuration, _}

/**
 * Created by loicmdivad.
 */
case class AccountActor(producer: ActorRef, topic: String, id: AccountId, var account: Account) extends Actor {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def delay: FiniteDuration = Gen.choose(1 second, 5 second).pureApply(Parameters.default, Seed.apply(24))

  override def receive: Receive = {

    case Start =>
      logger info s"Actor ready to send account update for ${account.login}"
      context.system.scheduler.scheduleOnce(5 second, self, Update)(context.dispatcher)

    case Stop =>
      logger info s"Actor stops sending update for ${account.login} (last update: ${account.lastUpdate})"

    case Update =>
      if(AccountActor.hasActivity(id.value)) {
        account = account.copy(lastUpdate = AccountUpdate(), plan = AccountActor.randomPlan)
        producer ! Message(new ProducerRecord[AccountId, Account](topic, id, account), NotUsed)
      }
      context.system.scheduler.scheduleOnce(delay, self, Update)(context.dispatcher)

    case other =>
      logger warn s"Unknown message was received: $other"
  }
}


object AccountActor {

  sealed trait AccountMessage
  case object Start extends AccountMessage
  case object Stop extends AccountMessage
  case object Update extends AccountMessage

  val activeAccountsProbabilities: Map[String, Gen[Boolean]] = Map(
    "ID-000" -> Gen.frequency((9, true), (1, false)),
    "ID-001" -> Gen.frequency((7, true), (3, false)),
    "ID-002" -> Gen.frequency((5, true), (5, false)),
    "ID-003" -> Gen.frequency((3, true), (7, false))
  )

  def hasActivity(id: String): Boolean =
    activeAccountsProbabilities(id).pureApply(Gen.Parameters.default, Seed.random())

  def randomPlan: Option[Account.Plan] =
    Gen.oneOf(Gold, Plus, Free).apply(Gen.Parameters.default, Seed.random())
}