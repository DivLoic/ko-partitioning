package fr.xebia.ldi.crocodile.schema

import java.time.{LocalDateTime, ZoneId}

import com.sksamuel.avro4s.{AvroName, RecordFormat}
import fr.xebia.ldi.crocodile.schema.Account.{AccountUpdate, Plan}

/**
 * Created by loicmdivad.
 */
case class Account(login: String, plan: Plan, @AvroName("last_update") lastUpdate: Option[AccountUpdate])

object Account extends ZoneIdConverter {

  implicit val recordFormat: RecordFormat[Account] = RecordFormat[Account]

  sealed trait Plan
  object Plus extends Plan
  object Gold extends Plan
  object Silver extends Plan
  object Free extends Plan

  case class AccountUpdate(datetime: LocalDateTime, zoneId: ZoneId)

  object AccountUpdate {
    def apply(): AccountUpdate = new AccountUpdate(LocalDateTime.now(), ZoneId.systemDefault())
  }
}