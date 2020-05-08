package fr.xebia.ldi.crocodile.schema

import com.sksamuel.avro4s.{AvroName, RecordFormat}
import fr.xebia.ldi.crocodile.schema.Account.{AccountUpdate, Plan}

/**
 * Created by loicmdivad.
 */
case class UserEvent(@AvroName("page_id")
                     pageId: String,
                     login: String,
                     plan: Option[Plan],
                     @AvroName("last_update")
                     lastUpdate: AccountUpdate) {

  override def toString: String =
    s"event: ${pageId.take(6)}..., plan: ${plan.orElse(None).map(_.toString.padTo(6, " ").mkString).orNull} ($login)"

}

object UserEvent extends ZoneIdConverter {

  implicit val recordFormat: RecordFormat[UserEvent] = RecordFormat[UserEvent]

  def apply(account: Account, click: Click): UserEvent =
    new UserEvent(click.pageId, account.login, account.plan, account.lastUpdate)
}