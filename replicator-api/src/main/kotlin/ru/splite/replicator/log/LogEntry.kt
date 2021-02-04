package ru.splite.replicator.log

import kotlinx.serialization.Serializable

@Serializable
data class LogEntry(val term: Long, val command: ByteArray) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LogEntry

        if (term != other.term) return false
        if (!command.contentEquals(other.command)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = term.hashCode()
        result = 31 * result + command.contentHashCode()
        return result
    }
}
