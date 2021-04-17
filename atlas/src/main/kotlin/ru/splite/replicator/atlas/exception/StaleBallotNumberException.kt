package ru.splite.replicator.atlas.exception

class StaleBallotNumberException(currentBallot: Long, messageBallot: Long) : RuntimeException(
    "Ballot invariant violated: currentBallot $currentBallot != messageBallot $messageBallot"
)