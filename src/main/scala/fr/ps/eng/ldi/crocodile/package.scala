package fr.ps.eng.ldi

import java.util

import com.sksamuel.avro4s
import com.sksamuel.avro4s.RecordFormat
import fr.ps.eng.ldi.crocodile.schema.{Account, Click, UserEvent, ZoneIdConverter}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

/**
 * Created by loicmdivad.
 */
package object crocodile extends ZoneIdConverter {

  trait GenericSerializer {
    val inner = new GenericAvroSerializer()
  }

  trait GenericDeserializer {
    val inner = new GenericAvroDeserializer()
  }

  def typedSerde[T: RecordFormat]: Serde[T] = Serdes.serdeFrom(
    new Serializer[T] with GenericSerializer {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        inner.configure(configs, isKey)

      override def serialize(topic: String, maybeData: T): Array[Byte] =
        Option(maybeData)
          .map(data => inner.serialize(topic, implicitly[RecordFormat[T]].to(data)))
          .getOrElse(Array.emptyByteArray)

      override def close(): Unit = inner.close()
    },

    new Deserializer[T] with GenericDeserializer {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        inner.configure(configs, isKey)

      override def deserialize(topic: String, maybeData: Array[Byte]): T =
        Option(maybeData)
          .filter(_.nonEmpty)
          .map(data => implicitly[RecordFormat[T]].from(inner.deserialize(topic, data)))
          .getOrElse(null.asInstanceOf[T])

      override def close(): Unit = inner.close()
    }
  )

  val schemaNameMap: Map[String, Schema] = Map(
    "Click" -> avro4s.AvroSchema[Click],
    "Account" -> avro4s.AvroSchema[Account],
    "UserEvent" -> avro4s.AvroSchema[UserEvent]
  )

}
