package ru.splite.replicator.atlas.state

import ru.splite.replicator.atlas.AtlasMessage

class SynodState(
    var ballot: Long = 0,
    var acceptedBallot: Long = 0,
    var consensusValue: AtlasMessage.ConsensusValue? = null
)