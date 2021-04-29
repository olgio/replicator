package ru.splite.replicator.raft.state

data class ExternalNodeState(val nextIndex: Long, val matchIndex: Long)