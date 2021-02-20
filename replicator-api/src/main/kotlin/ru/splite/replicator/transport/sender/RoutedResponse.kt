package ru.splite.replicator.transport.sender

import ru.splite.replicator.bus.NodeIdentifier

data class RoutedResponse<T>(val dst: NodeIdentifier, val response: T)