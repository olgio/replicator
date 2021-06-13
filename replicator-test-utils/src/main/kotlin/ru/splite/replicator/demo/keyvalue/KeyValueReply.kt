package ru.splite.replicator.demo.keyvalue

import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.transport.CommandSerializer

@Serializable
data class KeyValueReply internal constructor(
    val key: String,
    val value: String,
    val isEmpty: Boolean
) {

    companion object Serializer : CommandSerializer<KeyValueReply> {
        fun create(key: String, value: String?): KeyValueReply {
            if (value == null) {
                //TODO remove an empty string when kotlinx.serialization will support null for binary formats
                return KeyValueReply(key, "", true)
            }
            return KeyValueReply(key, value, false)
        }

        override fun serialize(command: KeyValueReply): ByteArray {
            return ProtoBuf.encodeToByteArray(serializer(), command)
        }

        override fun deserializer(byteArray: ByteArray): KeyValueReply {
            return ProtoBuf.decodeFromByteArray(serializer(), byteArray)
        }
    }
}
