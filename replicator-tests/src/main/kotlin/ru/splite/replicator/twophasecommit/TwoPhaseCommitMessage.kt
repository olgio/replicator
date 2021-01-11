package ru.splite.replicator.twophasecommit

import ru.splite.replicator.bus.NodeIdentifier

sealed class TwoPhaseCommitMessage<T>(open val from: NodeIdentifier) {

    data class AckRequest<T>(
        override val from: NodeIdentifier,
        val guid: String,
        val command: T
    ) : TwoPhaseCommitMessage<T>(from)

    data class AckResponse<T>(
        override val from: NodeIdentifier,
        val guid: String
    ) : TwoPhaseCommitMessage<T>(from)

    data class RejResponse<T>(
        override val from: NodeIdentifier,
        val guid: String
    ) : TwoPhaseCommitMessage<T>(from)

    data class Commit<T>(
        override val from: NodeIdentifier,
        val guid: String
    ) : TwoPhaseCommitMessage<T>(from)

    data class Rollback<T>(
        override val from: NodeIdentifier,
        val guid: String
    ) : TwoPhaseCommitMessage<T>(from)
}
