package ru.splite.replicator.raft

import ru.splite.replicator.transport.NodeIdentifier

data class RaftProtocolConfig(
    val address: NodeIdentifier,
    val n: Int,
    val leaderElectionQuorumSize: Int = n.asMajority(),
    val logReplicationQuorumSize: Int = n.asMajority(),
    val sendMessageTimeout: Long = 1000,
    val commandExecutorTimeout: Long = 3000,
    val appendEntriesSendPeriod: LongRange = 1000L..1000L,
    val termClockPeriod: LongRange = 4000L..8000L
) {

    init {
        check(leaderElectionQuorumSize + logReplicationQuorumSize > n) {
            "Quorum requirement violation: $leaderElectionQuorumSize + $logReplicationQuorumSize >= $n"
        }
    }
}