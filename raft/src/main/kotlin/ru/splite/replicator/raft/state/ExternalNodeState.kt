package ru.splite.replicator.raft.state

import kotlinx.serialization.Serializable

@Serializable
data class ExternalNodeState(val nextIndex: Long, val matchIndex: Long)