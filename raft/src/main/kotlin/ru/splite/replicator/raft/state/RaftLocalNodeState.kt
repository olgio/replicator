package ru.splite.replicator.raft.state

import ru.splite.replicator.bus.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap

open class RaftLocalNodeState(
    val nodeIdentifier: NodeIdentifier,
    var currentTerm: Long = 0L
) {

    var lastVotedLeaderIdentifier: NodeIdentifier? = null

    var currentNodeType: NodeType = NodeType.FOLLOWER

    var leaderIdentifier: NodeIdentifier? = null

    val externalNodeStates: MutableMap<NodeIdentifier, ExternalNodeState> = ConcurrentHashMap()
}