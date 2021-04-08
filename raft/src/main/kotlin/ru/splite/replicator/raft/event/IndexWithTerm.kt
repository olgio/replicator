package ru.splite.replicator.raft.event

data class IndexWithTerm(val index: Long, val term: Long)