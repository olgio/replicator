package ru.splite.replicator.exception

class StaleBallotNumberException(currentBallot: Long, messageBallot: Long) : RuntimeException(
    "Ballot invariant violated: currentBallot $currentBallot != messageBallot $messageBallot"
)