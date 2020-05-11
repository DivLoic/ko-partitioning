package fr.ps.eng.ldi.crocodile

import fr.ps.eng.ldi.crocodile.schema.{Account, AccountId, Click, UserEvent}
import org.apache.kafka.common.serialization.Serde

/**
 * Created by loicmdivad.
 */
trait CrocoSerde {

  implicit val clickSerde: Serde[Click] = typedSerde[Click]
  implicit val accountSerde: Serde[Account] = typedSerde[Account]
  implicit val accountIdSerde: Serde[AccountId] = typedSerde[AccountId]
  implicit val userEventSerde: Serde[UserEvent] = typedSerde[UserEvent]
}
