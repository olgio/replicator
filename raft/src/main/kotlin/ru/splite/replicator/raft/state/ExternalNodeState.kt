package ru.splite.replicator.raft.state

data class ExternalNodeState(var nextIndex: Long, var matchIndex: Long)