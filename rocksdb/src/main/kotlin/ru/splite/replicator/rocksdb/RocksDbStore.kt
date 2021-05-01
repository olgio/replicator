package ru.splite.replicator.rocksdb

import com.google.common.primitives.Longs
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.rocksdb.*
import java.io.File

class RocksDbStore(
    file: File,
    columnFamilyNames: Collection<String>
) {

    data class KeyValue<K, V>(val key: K, val value: V)

    inner class ColumnFamilyStore(private val columnFamilyHandle: ColumnFamilyHandle) {

        fun put(key: String, value: ByteArray) {
            db.put(columnFamilyHandle, key.toByteArray(), value)
        }

        fun put(key: Long, value: ByteArray) {
            db.put(columnFamilyHandle, Longs.toByteArray(key), value)
        }

        fun getAsByteArray(key: String): ByteArray? {
            return db.get(columnFamilyHandle, key.toByteArray())
        }

        fun getAsByteArray(key: Long): ByteArray? {
            return db.get(columnFamilyHandle, Longs.toByteArray(key))
        }

        inline fun <reified T> getAsType(key: String, serializer: KSerializer<T>): T? {
            return getAsByteArray(key)?.decodeFromByteArray(serializer)
        }

        inline fun <reified T> putAsType(key: String, value: T, serializer: KSerializer<T>) {
            put(key, value.encodeToByteArray(serializer))
        }

        inline fun <reified T> getAsType(key: Long, serializer: KSerializer<T>): T? {
            return getAsByteArray(key)?.decodeFromByteArray(serializer)
        }

        inline fun <reified T> putAsType(key: Long, value: T, serializer: KSerializer<T>) {
            put(key, value.encodeToByteArray(serializer))
        }

        fun getAll(): Sequence<KeyValue<ByteArray, ByteArray>> {
            return db.newIterator(columnFamilyHandle).asSequence()
        }

        inline fun <reified V> getAll(
            valueSerializer: KSerializer<V>
        ): Sequence<KeyValue<String, V>> {
            return getAll().map {
                KeyValue(
                    String(it.key),
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

    fun createColumnFamilyStore(name: String): ColumnFamilyStore = ColumnFamilyStore(columnFamilyHandles[name]!!)

    companion object {
        val binaryFormat: BinaryFormat = ProtoBuf
    }
}