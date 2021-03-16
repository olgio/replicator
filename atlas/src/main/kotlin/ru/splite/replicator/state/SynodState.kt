package ru.splite.replicator.state

import ru.splite.replicator.AtlasMessage

class SynodState(
    var ballot: Long = 0,
    var acceptedBallot: Long = 0,
    var consensusValue: AtlasMessage.ConsensusValue? = null
)