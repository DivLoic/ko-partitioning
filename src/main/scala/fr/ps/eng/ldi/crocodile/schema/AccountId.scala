package fr.ps.eng.ldi.crocodile.schema

import com.sksamuel.avro4s.RecordFormat

/**
 * Created by loicmdivad.
 */
case class AccountId(value: String)

object AccountId {

  implicit val recordFormat: RecordFormat[AccountId] = RecordFormat[AccountId]
}
