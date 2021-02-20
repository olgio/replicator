package ru.splite.replicator

//class MessageSenderTests {
//
//    @Test
//    fun test(): Unit = runBlockingTest {
//
//        val transport = buildTransport()
//
//        repeat(5) {
//            transport.createActor { _, payload ->
//                delay(500)
//                payload
//            }
//        }
//
//        repeat(5) {
//            transport.createActor { _, payload ->
//                delay(10_000)
//                payload
//            }
//        }
//
//        val submitActor = transport.createActor { nodeIdentifier, payload ->
//            payload
//        }
//
//        val command = KeyValueCommand.PutValue("testkey", "testvalue")
//        val messageSender = MessageSender(
//            actor = submitActor,
//            kSerializer = KeyValueCommand.serializer()
//        )
//        val responses = messageSender.sendToQuorum(messageSender.getAllNodes(), 5) {
//            command
//        }
//
//        advanceTimeBy(500)
//        assertThat(responses).hasSize(5)
//    }
//
//    private fun CoroutineScope.buildTransport(): CoroutineChannelTransport {
//        return CoroutineChannelTransport(this)
//    }
//
//    private fun Transport.createActor(receiveAction: suspend Actor.(NodeIdentifier, ByteArray) -> ByteArray): Actor {
//        val nodeIdentifier = NodeIdentifier(UUID.randomUUID().toString())
//        return object : TypedActor(nodeIdentifier, this) {
//
//            override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray {
//                return receiveAction(src, payload)
//            }
//
//        }
//    }
//}