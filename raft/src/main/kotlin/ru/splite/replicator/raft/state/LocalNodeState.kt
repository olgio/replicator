package ru.splite.replicator.raft.state

import ru.splite.replicator.transport.NodeIdentifier

open class LocalNodeState(
    val currentTerm: Long = 0L,
    val currentNodeType: NodeType = NodeType.FOLLOWER,
    val leaderIdentifier: NodeIdentifier? = null,
    val lastVotedLeaderIdentifier: NodeIdentifier? = null
) {

    override fun toString(): String {
        return "RaftLocalNodeState(currentTerm=$currentTerm, lastVotedLeaderIdentifier=$lastVotedLeaderIdentifier, currentNodeType=$currentNodeType, leaderIdentifier=$leaderIdentifier)"
    }

    fun copy(
        currentTerm: Long = this.currentTerm,
        currentNodeType: NodeType = this.currentNodeType,
        leaderIdentifier: NodeIdentifier? = this.leaderIdentifier,
        lastVotedLeaderIdentifier: NodeIdentifier? = this.lastVotedLeaderIdentifier
    ): LocalNodeState =
        LocalNodeState(
            currentTerm,
            currentNodeType,
            leaderIdentifier,
            lastVotedLeaderIdentifier
        )
}