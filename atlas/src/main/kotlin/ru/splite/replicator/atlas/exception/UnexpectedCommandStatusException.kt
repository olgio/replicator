package ru.splite.replicator.atlas.exception

import ru.splite.replicator.atlas.state.CommandStatus

class UnexpectedCommandStatusException(currentStatus: CommandStatus, expectedStatus: CommandStatus) :
    RuntimeException(
        "Command status invariant violated: currentStatus $currentStatus != expectedStatus $expectedStatus"
    )