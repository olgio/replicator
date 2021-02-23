package ru.splite.replicator

class SynodState(
    var ballot: Long = 0,
    var acceptedBallot: Long = 0,
    var consensusValue: AtlasMessage.ConsensusValue? = null
)