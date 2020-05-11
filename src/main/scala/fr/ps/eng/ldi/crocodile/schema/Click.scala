package fr.ps.eng.ldi.crocodile.schema

import com.sksamuel.avro4s.{AvroName, RecordFormat}

/**
 * Created by loicmdivad.
 */
case class Click(@AvroName("page_id") pageId: String)

object Click {

  implicit val recordFormat: RecordFormat[Click] = RecordFormat[Click]
}
