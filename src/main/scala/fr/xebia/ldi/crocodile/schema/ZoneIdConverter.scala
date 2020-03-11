package fr.xebia.ldi.crocodile.schema

import java.time.ZoneId
import java.time.format.TextStyle
import java.util.Locale

import com.sksamuel.avro4s.{Decoder, Encoder, FieldMapper, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}

/**
 * Created by loicmdivad.
 */
trait ZoneIdConverter {

  implicit val zoneIdEncoder: Encoder[ZoneId] =
    (t: ZoneId, _: Schema, _: FieldMapper) => t.getDisplayName(TextStyle.NARROW, Locale.ENGLISH)

  implicit val zoneIdDecoder: Decoder[ZoneId] =
    (value: Any, _: Schema, _: FieldMapper) => ZoneId.of(value.toString)

  implicit val zoneIdSchemaFor: SchemaFor[ZoneId] = (_: FieldMapper) => SchemaBuilder.builder().stringType()

}
