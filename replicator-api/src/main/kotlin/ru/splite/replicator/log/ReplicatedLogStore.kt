package ru.splite.replicator.log

/**
 * Реплицируемый журнал
 */
interface ReplicatedLogStore {

    /**
     * Установка записи [logEntry] в слот с индексом [index]
     * @throws CommittedLogEntryOverrideException если слот [index] уже подтвержден
     * @throws LogGapException если перезапись приводит к пропуску в логе
     */
    fun setLogEntry(index: Long, logEntry: LogEntry)

    /**
     * Добавление записи [logEntry] в новый слот журнала
     * @return индекс добавленной записи
     */
    fun appendLogEntry(logEntry: LogEntry): Long

    /**
     * @return запись журнала с индексом [index]
     */
    fun getLogEntryByIndex(index: Long): LogEntry?

    /**
     * Обрезка журнала, начиная с [index] включительно
     * @return индекс последней записи после обрезки ([index] - 1 или null если журнал пуст)
     */
    fun prune(index: Long): Long?

    /**
     * Пометка записи с индексом [index] и всех предшествующих записей подтвержденными
     * @throws LogGapException если слот с индексом [index] или предыдущие слоты еще не заполнены
     * @return индекс последнего подтвержденного слота журнала после обновления
     */
    fun commit(index: Long): Long

    /**
     * @return индекс последнего заполненного слота журнала
     */
    fun lastLogIndex(): Long?

    /**
     * @return индекс последнего подтвержденного слота журнала
     */
    fun lastCommitIndex(): Long?
}