package ru.splite.replicator.executor

import kotlinx.coroutines.CompletableDeferred

internal class DeferredCommand(
    val command: ByteArray,
    val deferredResponse: CompletableDeferred<ByteArray>
)