package fr.xebia.ldi.crocodile.schema

import java.time.{LocalDateTime, ZoneId, ZoneOffset}

import com.sksamuel.avro4s.{AvroName, RecordFormat}
import fr.xebia.ldi.crocodile.schema.Account.{AccountUpdate, Plan}

/**
 * Created by loicmdivad.
 */
case class Account(login: String, plan: Option[Plan], @AvroName("last_update") lastUpdate: AccountUpdate)

object Account extends ZoneIdConverter {

  def apply(login: String, plan: Plan): Account = new Account(login, Some(plan), AccountUpdate())

  implicit val recordFormat: RecordFormat[Account] = RecordFormat[Account]

  sealed trait Plan
  case object Plus extends Plan
  case object Gold extends Plan
  case object Silver extends Plan
  case object Free extends Plan
  case object None extends Plan

  case class AccountUpdate(datetime: LocalDateTime, zoneId: ZoneId)

  object AccountUpdate {
    def apply(): AccountUpdate = new AccountUpdate(LocalDateTime.now(), ZoneId.of(ZoneOffset.UTC.getId))
  }
}