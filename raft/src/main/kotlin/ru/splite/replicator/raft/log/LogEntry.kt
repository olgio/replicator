package ru.splite.replicator.raft.log

data class LogEntry<C>(val term: Long, val command: C)
