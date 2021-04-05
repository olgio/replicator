package ru.splite.replicator.transport.sender

import ru.splite.replicator.transport.NodeIdentifier

data class RoutedResponse<T>(val dst: NodeIdentifier, val response: T)