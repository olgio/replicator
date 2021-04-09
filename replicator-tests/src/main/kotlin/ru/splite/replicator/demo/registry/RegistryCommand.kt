package ru.splite.replicator.demo.registry

sealed class RegistryCommand {

    data class PutValue(val value: Long) : RegistryCommand()

    data class IncValue(val delta: Long) : RegistryCommand()
}