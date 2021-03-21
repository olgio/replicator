package ru.splite.replicator.raft

import ru.splite.replicator.raft.state.asMajority

data class RaftProtocolConfig(
    val n: Int,
    val leaderElectionQuorumSize: Int = n.asMajority(),
    val logReplicationQuorumSize: Int = n.asMajority(),
    val sendMessageTimeout: Long = 1000
) {

    init {
        check(leaderElectionQuorumSize + logReplicationQuorumSize > n) {
            "Quorum requirement violation: $leaderElectionQuorumSize + $logReplicationQuorumSize >= $n"
        }
    }
}