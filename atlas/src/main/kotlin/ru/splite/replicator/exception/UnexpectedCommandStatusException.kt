package ru.splite.replicator.exception

import ru.splite.replicator.state.CommandState

class UnexpectedCommandStatusException(currentStatus: CommandState.Status, expectedStatus: CommandState.Status) :
    RuntimeException(
        "Command status invariant violated: currentStatus $currentStatus != expectedStatus $expectedStatus"
    )