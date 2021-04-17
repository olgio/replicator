package ru.splite.replicator.atlas.exception

import ru.splite.replicator.atlas.state.CommandState

class UnexpectedCommandStatusException(currentStatus: CommandState.Status, expectedStatus: CommandState.Status) :
    RuntimeException(
        "Command status invariant violated: currentStatus $currentStatus != expectedStatus $expectedStatus"
    )