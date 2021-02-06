package ru.splite.replicator.registry

import org.slf4j.LoggerFactory
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

    fun getCurrentValue(): Long = currentValue.get()

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}