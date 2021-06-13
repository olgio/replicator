package ru.splite.replicator.atlas.state

import kotlinx.serialization.Serializable

@Serializable
sealed class Command {

    val isNoop: Boolean
        get() = this is WithNoop

    @Serializable
    class WithPayload(val payload: ByteArray) : Command()

    @Serializable
    object WithNoop : Command()

    @Serializable
    object Empty : Command()
}