package ru.splite.replicator.log

import kotlinx.serialization.Serializable

@Serializable
class LogEntry(val term: Long, val command: ByteArray)
