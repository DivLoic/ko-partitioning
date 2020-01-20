package fr.xebia.ldi

import java.util

import com.sksamuel.avro4s.RecordFormat
import fr.xebia.ldi.crocodile.schema.{Account, Click}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

/**
 * Created by loicmdivad.
 */
package object crocodile {

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

        override def serialize(topic: String, data: T): Array[Byte] =
          inner.serialize(topic, implicitly[RecordFormat[T]].to(data))

        override def close(): Unit = inner.close()
      },

      new Deserializer[T] with GenericDeserializer {
        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
          inner.configure(configs, isKey)

        override def deserialize(topic: String, data: Array[Byte]): T =
          implicitly[RecordFormat[T]].from(inner.deserialize(topic, data))

        override def close(): Unit = inner.close()
      }
    )

  val schemaNameMap: Map[String, Object] = Map(

    "Click" -> Click,
    "Account" -> Account
  )

}
