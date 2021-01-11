package ru.splite.replicator.raft.state

data class ExternalNodeState(val nextLogIndex: Long? = null, val lastAcceptedLogIndex: Long? = null)