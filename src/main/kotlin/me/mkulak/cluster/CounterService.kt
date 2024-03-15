package me.mkulak.cluster

import io.aeron.ExclusivePublication
import io.aeron.Image
import io.aeron.cluster.codecs.CloseReason
import io.aeron.cluster.service.ClientSession
import io.aeron.cluster.service.Cluster
import io.aeron.cluster.service.ClusteredService
import io.aeron.logbuffer.Header
import org.agrona.DirectBuffer
import org.agrona.ExpandableArrayBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.IdleStrategy
import org.agrona.concurrent.SleepingIdleStrategy

class CounterService : ClusteredService {
    protected var cluster: Cluster? = null
    protected var idleStrategy: IdleStrategy = SleepingIdleStrategy()
    private val egressMessageBuffer: MutableDirectBuffer = ExpandableArrayBuffer()
    private var counter = 0L

    override fun onStart(cluster: Cluster, snapshotImage: Image?) {
        this.cluster = cluster
        this.idleStrategy = cluster.idleStrategy()
    }

    override fun onSessionOpen(session: ClientSession, timestamp: Long) {
        println("session open: $session")
    }

    override fun onSessionClose(session: ClientSession, timestamp: Long, closeReason: CloseReason) {
        println("session close: $session")
    }

    override fun onSessionMessage(
        session: ClientSession,
        timestamp: Long,
        buffer: DirectBuffer,
        offset: Int,
        length: Int,
        header: Header
    ) {
        val correlationId = buffer.getLong(offset)
        val delta = buffer.getLong(offset + 8)
        println("${Thread.currentThread().name} Got message id=$correlationId delta=$delta old value: $counter")
        counter += delta
        egressMessageBuffer.putLong(0, correlationId)
        egressMessageBuffer.putLong(8, counter)
        idleStrategy.reset()
        repeat(SEND_ATTEMPTS) {
            val result = session.offer(egressMessageBuffer, 0, 16)
            if (result > 0) return
            idleStrategy.idle()
        }
    }

    override fun onTimerEvent(correlationId: Long, timestamp: Long) {
    }

    override fun onTakeSnapshot(snapshotPublication: ExclusivePublication) {
    }

    override fun onRoleChange(newRole: Cluster.Role) {
        println("onRoleChange: $newRole")
    }

    override fun onTerminate(cluster: Cluster) {
        println("onTerminate")
    }

    companion object {
        private const val SEND_ATTEMPTS = 3
    }
}
