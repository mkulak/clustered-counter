package me.mkulak.me.mkulak.cluster

import io.aeron.cluster.client.AeronCluster
import io.aeron.cluster.client.EgressListener
import io.aeron.cluster.codecs.EventCode
import io.aeron.logbuffer.Header
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.agrona.DirectBuffer
import org.agrona.ExpandableArrayBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.IdleStrategy
import org.agrona.concurrent.SleepingIdleStrategy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

interface CounterClient {
    suspend fun get(): Long
    suspend fun add(delta: Long): Long
}

class CounterClientImpl : EgressListener, CounterClient {
    private val egressBuffer: MutableDirectBuffer = ExpandableArrayBuffer()
    private val idleStrategy: IdleStrategy = SleepingIdleStrategy()
    private var nextCorrelationId: Long = 0
    private lateinit var aeronCluster: AeronCluster
    private val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val scope = CoroutineScope(dispatcher)
    private val replies = ConcurrentHashMap<Long, Channel<Long>>()

    override fun onMessage(
        clusterSessionId: Long,
        timestamp: Long,
        buffer: DirectBuffer,
        offset: Int,
        length: Int,
        header: Header
    ) {
        val correlationId = buffer.getLong(offset)
        val counterValue = buffer.getLong(offset + 8)
        println("${Thread.currentThread().name} Client got message id=$correlationId value=$counterValue of length $length $replies")
        val channel = replies.remove(correlationId)
        if (channel != null) {
            println("replying to $correlationId")
            channel.trySend(counterValue)
        } else {
            println("no reply for $correlationId")
        }
    }

    override fun onSessionEvent(
        correlationId: Long,
        clusterSessionId: Long,
        leadershipTermId: Long,
        leaderMemberId: Int,
        code: EventCode,
        detail: String
    ) {
        println("SessionEvent($correlationId, $leadershipTermId, $leaderMemberId, $code, $detail)")
    }

    override fun onNewLeader(
        clusterSessionId: Long,
        leadershipTermId: Long,
        leaderMemberId: Int,
        ingressEndpoints: String
    ) {
        println("NewLeader($clusterSessionId, $leadershipTermId, $leaderMemberId)")
    }

    fun start(ctx: AeronCluster.Context) {
        aeronCluster = AeronCluster.connect(ctx)
        println("Cluster client connected")

        scope.launch {
            while (isActive) {
                delay(1000.nanoseconds)
                aeronCluster.pollEgress()
            }
        }
        scope.launch {
            while (isActive) {
                delay(2.seconds)
                aeronCluster.sendKeepAlive()
            }
        }
    }

    override suspend fun get(): Long = withContext(dispatcher) {
        add(0)
    }

    override suspend fun add(delta: Long): Long = withContext(dispatcher) {
        nextCorrelationId++
        val id = nextCorrelationId
        val channel = Channel<Long>(1)
        replies[id] = channel
        sendDelta(id, delta)
        channel.receive()
    }

    private fun sendDelta(id: Long, delta: Long) {
        println("sending message id=$id delta=$delta")
        egressBuffer.putLong(0, id)
        egressBuffer.putLong(8, delta)
        idleStrategy.reset()
        while (aeronCluster.offer(egressBuffer, 0, 16) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress())
        }
    }
}