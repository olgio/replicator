package ru.splite.replicator.atlas.state

import kotlinx.serialization.Serializable

@Serializable
sealed class Command {

    @Serializable
    class WithPayload(val payload: ByteArray) : Command()

    @Serializable
    object WithNoop : Command()

    @Serializable
    object Empty : Command()
}