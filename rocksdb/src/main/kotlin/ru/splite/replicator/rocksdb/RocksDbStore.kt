package ru.splite.replicator.rocksdb

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.rocksdb.*
import java.io.File
import kotlin.coroutines.CoroutineContext

class RocksDbStore(
    file: File,
    columnFamilyNames: Collection<String>,
    private val coroutineContext: CoroutineContext = Dispatchers.Unconfined
) {

    inner class ColumnFamilyStore(
        private val columnFamilyHandle: ColumnFamilyHandle,
        val binaryFormat: BinaryFormat
    ) {

        suspend fun put(key: ByteArray, value: ByteArray) = withContext(coroutineContext) {
            db.put(columnFamilyHandle, key, value)
        }

        suspend fun put(key: String, value: ByteArray) {
            put(key.toByteArray(), value)
        }

        suspend fun put(key: Long, value: ByteArray) {
            put(key.toByteArray(), value)
        }

        suspend fun getAsByteArray(key: ByteArray): ByteArray? = withContext(coroutineContext) {
            db.get(columnFamilyHandle, key)
        }

        suspend fun getAsByteArray(key: String): ByteArray? {
            return getAsByteArray(key.toByteArray())
        }

        suspend fun getAsByteArray(key: Long): ByteArray? {
            return getAsByteArray(key.toByteArray())
        }

        suspend fun delete(key: ByteArray) = withContext(coroutineContext) {
            db.delete(columnFamilyHandle, key)
        }

        suspend inline fun <reified K, reified T> getAsType(
            key: K,
            keySerializer: KSerializer<K>,
            serializer: KSerializer<T>
        ): T? {
            return getAsByteArray(
                key.encodeToByteArray(keySerializer)
            )?.decodeFromByteArray(serializer)
        }

        suspend inline fun <reified K, reified T> putAsType(
            key: K,
            value: T,
            keySerializer: KSerializer<K>,
            serializer: KSerializer<T>
        ) {
            put(key.encodeToByteArray(keySerializer), value.encodeToByteArray(serializer))
        }

        suspend inline fun <reified K> delete(
            key: K,
            keySerializer: KSerializer<K>
        ) {
            return delete(key.encodeToByteArray(keySerializer))
        }

        suspend inline fun <reified T> getAsType(key: String, serializer: KSerializer<T>): T? {
            return getAsByteArray(key)?.decodeFromByteArray(serializer)
        }

        suspend inline fun <reified T> putAsType(key: String, value: T, serializer: KSerializer<T>) {
            put(key, value.encodeToByteArray(serializer))
        }

        suspend inline fun <reified T> getAsType(key: Long, serializer: KSerializer<T>): T? {
            return getAsByteArray(key)?.decodeFromByteArray(serializer)
        }

        suspend inline fun <reified T> putAsType(key: Long, value: T, serializer: KSerializer<T>) {
            put(key, value.encodeToByteArray(serializer))
        }

        suspend fun getAll(): Sequence<KeyValue<ByteArray, ByteArray>> = withContext(coroutineContext) {
            db.newIterator(columnFamilyHandle).asSequence()
        }

        suspend inline fun <reified V> getAll(
            valueSerializer: KSerializer<V>
        ): Sequence<KeyValue<String, V>> {
            return getAll().map {
                KeyValue(
                    String(it.key),
                    it.value.decodeFromByteArray(valueSerializer)
                )
            }
        }

        suspend inline fun <reified K, reified V> getAll(
            keySerializer: KSerializer<K>,
            valueSerializer: KSerializer<V>
        ): Sequence<KeyValue<K, V>> {
            return getAll().map {
                KeyValue(
                    it.key.decodeFromByteArray(keySerializer),
                    it.value.decodeFromByteArray(valueSerializer)
                )
            }
        }

        private fun RocksIterator.asSequence(): Sequence<KeyValue<ByteArray, ByteArray>> {
            return sequence {
                this@asSequence.use {
                    it.seekToFirst()
                    while (it.isValid) {
                        yield(KeyValue(it.key(), it.value()))
                        it.next()
                    }
                }
            }
        }

        inline fun <reified V> V.encodeToByteArray(serializer: KSerializer<V>): ByteArray =
            binaryFormat.encodeToByteArray(serializer, this)

        inline fun <reified V> ByteArray.decodeFromByteArray(serializer: KSerializer<V>): V =
            binaryFormat.decodeFromByteArray(serializer, this)
    }

    private val columnFamilyDescriptors: List<ColumnFamilyDescriptor> = listOf(
        ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
    ).plus(columnFamilyNames.map { ColumnFamilyDescriptor(it.toByteArray()) })

    private val columnFamilyHandles: Map<String, ColumnFamilyHandle>

    private val db: RocksDB

    init {
        RocksDB.loadLibrary()
        val options = DBOptions().apply {
            setCreateIfMissing(true)
            setCreateMissingColumnFamilies(true)
            setErrorIfExists(false)
        }
        val columnFamilyHandlesArray = ArrayList<ColumnFamilyHandle>(columnFamilyNames.size)
        db = RocksDB.open(options, file.absolutePath, columnFamilyDescriptors, columnFamilyHandlesArray)
        columnFamilyHandles = columnFamilyNames.mapIndexed { index, name ->
            name to columnFamilyHandlesArray[index]
        }.toMap()
    }

    fun createColumnFamilyStore(name: String, binaryFormat: BinaryFormat = ProtoBuf): ColumnFamilyStore =
        ColumnFamilyStore(columnFamilyHandles[name]!!, binaryFormat)
}