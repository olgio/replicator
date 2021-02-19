package ru.splite.replicator.keyvalue

import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.transport.CommandSerializer

@Serializable
data class KeyValueReply(val key: String, val value: String?) {

    companion object Serializer : CommandSerializer<KeyValueReply> {
        override fun serialize(command: KeyValueReply): ByteArray {
            return ProtoBuf.encodeToByteArray(serializer(), command)
        }

        override fun deserializer(byteArray: ByteArray): KeyValueReply {
            return ProtoBuf.decodeFromByteArray(serializer(), byteArray)
        }
    }
}
