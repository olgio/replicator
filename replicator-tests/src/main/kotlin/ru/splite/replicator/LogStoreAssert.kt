package ru.splite.replicator

import org.assertj.core.api.Assertions
import ru.splite.replicator.log.ReplicatedLogStore

class LogStoreAssert<C>(private val logStores: List<ReplicatedLogStore<C>>) {

    fun hasOnlyTerms(vararg values: Long): LogStoreAssert<C> {
        logStores.forEach { logStore ->
            Assertions.assertThat(logStore.fullLogSize).isEqualTo(values.size.toLong())
            values.forEachIndexed { index, value ->
                Assertions.assertThat(logStore.getLogEntryByIndex(index.toLong())?.term).isEqualTo(value)
            }
        }
        return this
    }

    fun hasOnlyEntries(vararg values: C): LogStoreAssert<C> {
        logStores.forEach { logStore ->
            Assertions.assertThat(logStore.fullLogSize).isEqualTo(values.size.toLong())
            values.forEachIndexed { index, value ->
                Assertions.assertThat(logStore.getLogEntryByIndex(index.toLong())?.command).isEqualTo(value)
            }
        }
        return this
    }

    fun hasCommittedEntriesSize(committedSize: Long): LogStoreAssert<C> {
        logStores.forEach { logStore ->
            Assertions.assertThat(logStore.lastCommitIndex()?.plus(1L) ?: 0L).isEqualTo(committedSize)
        }
        return this
    }

    fun isCommittedEntriesInSync(): LogStoreAssert<C> {
        val lastCommitIndex: Long? = logStores.firstOrNull()?.lastCommitIndex()

        logStores.forEach { logStore ->
            Assertions.assertThat(logStore.lastCommitIndex()).isEqualTo(lastCommitIndex)
        }

        if (lastCommitIndex != null) {
            (0..lastCommitIndex).forEach { index ->
                Assertions.assertThat(logStores.map {
                    it.getLogEntryByIndex(index)
                }.toSet()).hasSize(1)
            }
        }
        return this
    }

    private val ReplicatedLogStore<C>.fullLogSize: Long
        get() = this.lastLogIndex()?.plus(1L) ?: 0L

    companion object {

        fun <C> assertThatLogs(vararg logStores: ReplicatedLogStore<C>): LogStoreAssert<C> {
            return LogStoreAssert(logStores.toList())
        }
    }
}