package ru.splite.replicator

import ru.splite.replicator.bus.NodeIdentifier

class CommandState(
    var status: Status = Status.START,
    var quorum: Set<NodeIdentifier> = emptySet(),
    var command: ByteArray? = null
) {

//    val quorumDependencies by lazy {
//        QuorumDependencies()
//    }

    val synodState by lazy {
        SynodState()
    }

    enum class Status { START, COLLECT, PAYLOAD, COMMIT }
}
