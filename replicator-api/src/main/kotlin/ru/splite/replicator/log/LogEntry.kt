package ru.splite.replicator.log

data class LogEntry<C>(val term: Long, val command: C)
