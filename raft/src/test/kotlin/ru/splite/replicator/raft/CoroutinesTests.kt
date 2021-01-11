package ru.splite.replicator.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.concurrent.fixedRateTimer
import kotlin.test.Test

class CoroutinesTests {

    @Test
    fun stateFlowTest() {
        runBlocking(newFixedThreadPoolContext(2, "first")) {
            val stateFlow = MutableStateFlow(0)

            launch {
                stateFlow.collect { i ->
                    println("${Thread.currentThread().name} collected $i")
                }
            }

            println("s")


            launch {
                (1..1000).forEach { i ->
                    stateFlow.value = i

                    println("${Thread.currentThread().name} set $i")

                }
            }
        }
    }

    @Test
    fun channelTest() = runBlocking(newFixedThreadPoolContext(1, "first")) {
        val channel = Channel<Int>(capacity = 100)

        launch {
            (1..1000).forEach { i ->
                channel.send(i)

                println("${Thread.currentThread().name} emit $i")
            }
        }

        withContext(newFixedThreadPoolContext(1, "second")) {
            for (i in channel) {
                println("${Thread.currentThread().name} collected $i")
            }
        }

        delay(10 * 1000)
    }

    @Test
    fun callbackFlowTest() {
        callbackFlow<Int> {
            runBlocking(newFixedThreadPoolContext(5, "first")) {
                launch {
                    (1..1000).forEach { i ->
                        channel.send(i)
                        println("${Thread.currentThread().name} emit $i")
                    }
                }
            }
        }
    }

    @Test
    fun flowTest() = runBlocking(newFixedThreadPoolContext(1, "first")) {

        val flow = flow<Int> {
            (1..1000).forEach { i ->
                emit(i)

                println("${Thread.currentThread().name} emit $i")

            }
        }.buffer(100).flowOn(newFixedThreadPoolContext(1, "third"))

        withContext(newFixedThreadPoolContext(5, "second")) {
            flow.collect {
                delay(1000)
                println("${Thread.currentThread().name} collected $it")
            }
        }

        delay(10 * 1000)
    }

    @Test
    fun delayTest() {
        runBlocking {
            println("1")
            delay(100000)
            launch {
                delay(1000)
                println("3")
            }
            delay(1000)
            println("2")
        }
    }

    @Test
    fun timerTest() {
        runBlocking(newFixedThreadPoolContext(name = "my", nThreads = 5)) {
            fixedRateTimer(period = 500L) {
                println("${Thread.currentThread().name} point0")

                val scope = this@runBlocking
                runBlocking {
                    scope.launch {
                        println("${Thread.currentThread().name} point1")
                        //Thread.sleep(5000)
                        println("${Thread.currentThread().name} point1+1")
                    }
                }
                println("${Thread.currentThread().name} point2")

            }
            println("${Thread.currentThread().name} ${this.coroutineContext}")

            delay(10 * 1000)
        }
    }
}