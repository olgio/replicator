package ru.splite.replicator.registry

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.StateMachine
import java.util.concurrent.atomic.AtomicLong

class RegistryStateMachine : StateMachine<RegistryCommand, Unit> {

    private val currentValue = AtomicLong(0)

    override fun commit(command: RegistryCommand) {
        val newValue = when (command) {
            is RegistryCommand.PutValue -> {
                currentValue.set(command.value)
                command.value
            }
            is RegistryCommand.IncValue -> currentValue.addAndGet(command.delta)
        }
        LOGGER.info("Applied command {}. newValue = {}", command, newValue)
    }

    override fun <K> newConflictIndex(): ConflictIndex<K, RegistryCommand> {
        TODO("Not yet implemented")
    }

    fun getCurrentValue(): Long = currentValue.get()

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}