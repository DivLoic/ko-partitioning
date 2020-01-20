package fr.xebia.ldi.crocodile

import fr.xebia.ldi.crocodile.schema.{Account, AccountId, Click}
import org.apache.kafka.common.serialization.Serde

/**
 * Created by loicmdivad.
 */
trait CrocoSerde {

  val clickSerde: Serde[Click] = typedSerde[Click]
  val accountSerde: Serde[Account] = typedSerde[Account]
  val accountIdSerde: Serde[AccountId] = typedSerde[AccountId]
}
