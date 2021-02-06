package ru.splite.replicator.transport

class NodeUnavailableException : RuntimeException {

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}