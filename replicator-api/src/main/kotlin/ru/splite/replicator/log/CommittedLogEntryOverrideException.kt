package ru.splite.replicator.log

class CommittedLogEntryOverrideException : RuntimeException {

    constructor(message: String) : super(message)

    constructor(message: String, cause: Throwable) : super(message, cause)
}