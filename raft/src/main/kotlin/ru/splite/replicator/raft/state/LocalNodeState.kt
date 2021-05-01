package ru.splite.replicator.raft.state

import kotlinx.serialization.Serializable
import ru.splite.replicator.transport.NodeIdentifier

@Serializable
data class LocalNodeState(
    val currentTerm: Long = 0L,
    val currentNodeType: NodeType = NodeType.FOLLOWER,
    val leaderIdentifier: NodeIdentifier? = null,
    val lastVotedLeaderIdentifier: NodeIdentifier? = null
)