package ru.splite.replicator.paxos.state

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.raft.state.RaftLocalNodeState

class PaxosLocalNodeState<C>(nodeIdentifier: NodeIdentifier, currentTerm: Long) :
    RaftLocalNodeState(nodeIdentifier, currentTerm) {

    val entries: MutableList<LogEntry<C>> = mutableListOf()
}