package me.mkulak.cluster

import io.aeron.CommonContext
import io.aeron.archive.Archive
import io.aeron.archive.ArchiveThreadingMode
import io.aeron.archive.client.AeronArchive
import io.aeron.cluster.ConsensusModule
import io.aeron.cluster.service.ClusteredService
import io.aeron.cluster.service.ClusteredServiceContainer
import io.aeron.driver.MediaDriver
import io.aeron.driver.MinMulticastFlowControlSupplier
import io.aeron.driver.ThreadingMode
import org.agrona.ErrorHandler
import org.agrona.concurrent.NoOpLock
import java.io.File

class ClusterConfig(
    private val memberId: Int,
    private val ingressHostname: String,
    private val clusterHostname: String,
    private val mediaDriverContext: MediaDriver.Context,
    private val archiveContext: Archive.Context,
    private val aeronArchiveContext: AeronArchive.Context,
    private val consensusModuleContext: ConsensusModule.Context,
    private val clusteredServiceContexts: List<ClusteredServiceContainer.Context>
) {
    fun errorHandler(errorHandler: ErrorHandler?) {
        mediaDriverContext.errorHandler(errorHandler)
        archiveContext.errorHandler(errorHandler)
        aeronArchiveContext.errorHandler(errorHandler)
        consensusModuleContext.errorHandler(errorHandler)
        clusteredServiceContexts.forEach { it.errorHandler(errorHandler) }
    }

    /**
     * Set the aeron directory for all configuration contexts.
     *
     * @param aeronDir directory to use for aeron.
     */
    fun aeronDirectoryName(aeronDir: String?) {
        mediaDriverContext.aeronDirectoryName(aeronDir)
        archiveContext.aeronDirectoryName(aeronDir)
        aeronArchiveContext.aeronDirectoryName(aeronDir)
        consensusModuleContext.aeronDirectoryName(aeronDir)
        clusteredServiceContexts.forEach { it.aeronDirectoryName(aeronDir) }
    }

    /**
     * Set the base directory for cluster and archive.
     *
     * @param baseDir parent directory to be used for archive and cluster stored data.
     */
    fun baseDir(baseDir: File?) {
        archiveContext.archiveDir(File(baseDir, ARCHIVE_SUB_DIR))
        consensusModuleContext.clusterDir(File(baseDir, CLUSTER_SUB_DIR))
        clusteredServiceContexts.forEach { it.clusterDir(File(baseDir, CLUSTER_SUB_DIR)) }
    }

    /**
     * Gets the configuration's media driver context.
     *
     * @return configured [MediaDriver.Context].
     * @see MediaDriver.Context
     */
    fun mediaDriverContext(): MediaDriver.Context {
        return mediaDriverContext
    }

    /**
     * Gets the configuration's archive context.
     *
     * @return configured [Archive.Context].
     * @see Archive.Context
     */
    fun archiveContext(): Archive.Context {
        return archiveContext
    }

    /**
     * Gets the configuration's aeron archive context.
     *
     * @return configured [Archive.Context].
     * @see AeronArchive.Context
     */
    fun aeronArchiveContext(): AeronArchive.Context {
        return aeronArchiveContext
    }

    /**
     * Gets the configuration's consensus module context.
     *
     * @return configured [ConsensusModule.Context].
     * @see ConsensusModule.Context
     */
    fun consensusModuleContext(): ConsensusModule.Context {
        return consensusModuleContext
    }

    /**
     * Gets the configuration's clustered service container context.
     *
     * @return configured [ClusteredServiceContainer.Context].
     * @see ClusteredServiceContainer.Context
     */
    fun clusteredServiceContext(): ClusteredServiceContainer.Context {
        return clusteredServiceContexts[0]
    }

    /**
     * Gets the configuration's list of clustered service container contexts.
     *
     * @return configured list of [ClusteredServiceContainer.Context].
     * @see ClusteredServiceContainer.Context
     */
    fun clusteredServiceContexts(): List<ClusteredServiceContainer.Context> {
        return clusteredServiceContexts
    }

    /**
     * memberId of this node.
     *
     * @return memberId.
     */
    fun memberId(): Int {
        return memberId
    }

    /**
     * Hostname of this node that will receive ingress traffic.
     *
     * @return ingress hostname.
     */
    fun ingressHostname(): String {
        return ingressHostname
    }

    /**
     * Hostname of this node that will receive cluster traffic.
     *
     * @return cluster hostname
     */
    fun clusterHostname(): String {
        return clusterHostname
    }

    companion object {
        const val PORTS_PER_NODE: Int = 100
        const val ARCHIVE_CONTROL_PORT_OFFSET: Int = 1
        const val CLIENT_FACING_PORT_OFFSET: Int = 2
        const val MEMBER_FACING_PORT_OFFSET: Int = 3
        const val LOG_PORT_OFFSET: Int = 4
        const val TRANSFER_PORT_OFFSET: Int = 5
        const val ARCHIVE_SUB_DIR: String = "archive"
        const val CLUSTER_SUB_DIR: String = "cluster"

        /**
         * Create a new ClusterConfig. This call allows for 2 separate lists of hostnames, so that there can be 'external'
         * addresses for ingress requests and 'internal' addresses that will handle all the cluster replication and
         * control traffic.
         *
         * @param startingMemberId   id for the first member in the list of entries.
         * @param memberId           id for this node.
         * @param ingressHostnames   list of hostnames that will receive ingress request traffic.
         * @param clusterHostnames   list of hostnames that will receive cluster traffic.
         * @param portBase           base port to derive remaining ports from.
         * @param parentDir          directory under which the persistent directories will be created.
         * @param clusteredService   instance of the clustered service that will run with this configuration.
         * @param additionalServices instances of additional clustered services that will run with this configuration.
         * @return configuration that wraps all aeron service configuration.
         */
        fun create(
            startingMemberId: Int,
            memberId: Int,
            ingressHostnames: List<String>,
            clusterHostnames: List<String>,
            portBase: Int,
            parentDir: File?,
            clusteredService: ClusteredService?,
            vararg additionalServices: ClusteredService?
        ): ClusterConfig {
            require(!(memberId < startingMemberId || (startingMemberId + ingressHostnames.size) <= memberId)) {
                "memberId=" + memberId + " is invalid, should be " + startingMemberId +
                    " <= memberId < " + startingMemberId + ingressHostnames.size
            }

            val clusterMembers = clusterMembers(startingMemberId, ingressHostnames, clusterHostnames, portBase)

            val aeronDirName = CommonContext.getAeronDirectoryName() + "-" + memberId + "-driver"
            val baseDir = File(parentDir, "aeron-cluster-$memberId")

            val ingressHostname = ingressHostnames[memberId - startingMemberId]
            val hostname = clusterHostnames[memberId - startingMemberId]

            val mediaDriverContext = MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(MinMulticastFlowControlSupplier())

            val replicationArchiveContext = AeronArchive.Context()
                .controlResponseChannel("aeron:udp?endpoint=$hostname:0")

            val archiveContext = Archive.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveDir(File(baseDir, ARCHIVE_SUB_DIR))
                .controlChannel(udpChannel(memberId, hostname, portBase, ARCHIVE_CONTROL_PORT_OFFSET))
                .archiveClientContext(replicationArchiveContext)
                .localControlChannel("aeron:ipc?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=$hostname:0")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)

            val aeronArchiveContext = AeronArchive.Context()
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.localControlChannel())
                .controlRequestStreamId(archiveContext.localControlStreamId())
                .controlResponseChannel(archiveContext.localControlChannel())
                .aeronDirectoryName(aeronDirName)

            val consensusModuleContext = ConsensusModule.Context()
                .clusterMemberId(memberId)
                .clusterMembers(clusterMembers)
                .clusterDir(File(baseDir, CLUSTER_SUB_DIR))
                .archiveContext(aeronArchiveContext.clone())
                .serviceCount(1 + additionalServices.size)
                .replicationChannel("aeron:udp?endpoint=$hostname:0")

            val serviceContexts: MutableList<ClusteredServiceContainer.Context> = ArrayList()

            val clusteredServiceContext = ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(File(baseDir, CLUSTER_SUB_DIR))
                .clusteredService(clusteredService)
                .serviceId(0)
            serviceContexts.add(clusteredServiceContext)

            for (i in additionalServices.indices) {
                val additionalServiceContext = ClusteredServiceContainer.Context()
                    .aeronDirectoryName(aeronDirName)
                    .archiveContext(aeronArchiveContext.clone())
                    .clusterDir(File(baseDir, CLUSTER_SUB_DIR))
                    .clusteredService(additionalServices[i])
                    .serviceId(i + 1)
                serviceContexts.add(additionalServiceContext)
            }

            return ClusterConfig(
                memberId,
                ingressHostname,
                hostname,
                mediaDriverContext,
                archiveContext,
                aeronArchiveContext,
                consensusModuleContext,
                serviceContexts
            )
        }

        /**
         * Create a new ClusterConfig. This call allows for 2 separate lists of hostnames, so that there can be 'external'
         * addresses for ingress requests and 'internal' addresses that will handle all the cluster replication and
         * control traffic.
         *
         * @param nodeId             id for this node.
         * @param ingressHostnames   list of hostnames that will receive ingress request traffic.
         * @param clusterHostnames   list of hostnames that will receive cluster traffic.
         * @param portBase           base port to derive remaining ports from.
         * @param clusteredService   instance of the clustered service that will run with this configuration.
         * @param additionalServices instances of additional clustered services that will run with this configuration.
         * @return configuration that wraps all aeron service configuration.
         */
        fun create(
            nodeId: Int,
            ingressHostnames: List<String>,
            clusterHostnames: List<String>,
            portBase: Int,
            clusteredService: ClusteredService?,
            vararg additionalServices: ClusteredService?
        ): ClusterConfig {
            return create(
                0,
                nodeId,
                ingressHostnames,
                clusterHostnames,
                portBase,
                File(System.getProperty("user.dir")),
                clusteredService,
                *additionalServices
            )
        }

        /**
         * Create a new ClusterConfig. This only supports a single lists of hostnames.
         *
         * @param nodeId           id for this node.
         * @param hostnames        list of hostnames that will receive ingress request and cluster traffic.
         * @param portBase         base port to derive remaining ports from.
         * @param clusteredService instance of the clustered service that will run on this node.
         * @return configuration that wraps the detailed aeron service configuration.
         */
        fun create(
            nodeId: Int,
            hostnames: List<String>,
            portBase: Int,
            clusteredService: ClusteredService?
        ): ClusterConfig {
            return create(nodeId, hostnames, hostnames, portBase, clusteredService)
        }

        /**
         * String representing the cluster members configuration which can be used for
         * [io.aeron.cluster.ClusterMember.parse].
         *
         * @param ingressHostnames of the cluster members.
         * @param clusterHostnames of the cluster members internal address (can be the same as 'hostnames').
         * @param portBase         initial port to derive other port from via appropriate node id and offset.
         * @return the String which can be used for [io.aeron.cluster.ClusterMember.parse].
         */
        fun clusterMembers(
            ingressHostnames: List<String>, clusterHostnames: List<String>, portBase: Int
        ): String {
            return clusterMembers(0, ingressHostnames, clusterHostnames, portBase)
        }

        /**
         * String representing the cluster members configuration which can be used for
         * [io.aeron.cluster.ClusterMember.parse].
         *
         * @param startingMemberId first memberId to be used in the list of clusterMembers. The memberId will increment by 1
         * from that value for each entry.
         * @param ingressHostnames of the cluster members.
         * @param clusterHostnames of the cluster members internal address (can be the same as 'hostnames').
         * @param portBase         initial port to derive other port from via appropriate node id and offset.
         * @return the String which can be used for [io.aeron.cluster.ClusterMember.parse].
         */
        fun clusterMembers(
            startingMemberId: Int,
            ingressHostnames: List<String>,
            clusterHostnames: List<String>,
            portBase: Int
        ): String {
            require(ingressHostnames.size == clusterHostnames.size) { "ingressHostnames and clusterHostnames must be the same size" }

            val sb = StringBuilder()
            for (i in ingressHostnames.indices) {
                val memberId = i + startingMemberId
                sb.append(memberId)
                sb.append(',').append(endpoint(memberId, ingressHostnames[i], portBase, CLIENT_FACING_PORT_OFFSET))
                sb.append(',').append(endpoint(memberId, clusterHostnames[i], portBase, MEMBER_FACING_PORT_OFFSET))
                sb.append(',').append(endpoint(memberId, clusterHostnames[i], portBase, LOG_PORT_OFFSET))
                sb.append(',').append(endpoint(memberId, clusterHostnames[i], portBase, TRANSFER_PORT_OFFSET))
                sb.append(',').append(endpoint(memberId, clusterHostnames[i], portBase, ARCHIVE_CONTROL_PORT_OFFSET))
                sb.append('|')
            }

            return sb.toString()
        }

        /**
         * Ingress endpoints generated from a list of hostnames.
         *
         * @param hostnames              for the cluster members.
         * @param portBase               Base port for the cluster
         * @param clientFacingPortOffset Offset for the client facing port
         * @return a formatted string of ingress endpoints for connecting to a cluster.
         */
        fun ingressEndpoints(
            hostnames: List<String?>,
            portBase: Int,
            clientFacingPortOffset: Int
        ): String {
            return ingressEndpoints(0, hostnames, portBase, clientFacingPortOffset)
        }

        /**
         * Ingress endpoints generated from a list of hostnames.
         *
         * @param startingMemberId       first memberId to be used when generating the ports.
         * @param hostnames              for the cluster members.
         * @param portBase               Base port for the cluster
         * @param clientFacingPortOffset Offset for the client facing port
         * @return a formatted string of ingress endpoints for connecting to a cluster.
         */
        fun ingressEndpoints(
            startingMemberId: Int,
            hostnames: List<String?>,
            portBase: Int,
            clientFacingPortOffset: Int
        ): String {
            val sb = StringBuilder()
            for (i in hostnames.indices) {
                val memberId = i + startingMemberId
                sb.append(memberId).append('=')
                sb.append(hostnames[i]).append(':').append(calculatePort(memberId, portBase, clientFacingPortOffset))
                sb.append(',')
            }

            sb.setLength(sb.length - 1)

            return sb.toString()
        }

        /**
         * Calculates a port for use with a node based on a specific offset.  Can be used with the predefined offsets, e.g.
         * [ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET] or with custom offsets.  For custom offsets select a value
         * larger than largest predefined offsets.  A value larger than the largest predefined offset, but less than
         * [ClusterConfig.PORTS_PER_NODE] is required.
         *
         * @param nodeId   The id for the member of the cluster.
         * @param portBase The port base to be used.
         * @param offset   The offset to add onto the port base
         * @return a calculated port, which should be unique for the specified criteria.
         */
        fun calculatePort(nodeId: Int, portBase: Int, offset: Int): Int {
            return portBase + (nodeId * PORTS_PER_NODE) + offset
        }

        private fun udpChannel(nodeId: Int, hostname: String, portBase: Int, portOffset: Int): String {
            val port = calculatePort(nodeId, portBase, portOffset)
            return "aeron:udp?endpoint=$hostname:$port"
        }

        private fun endpoint(nodeId: Int, hostname: String, portBase: Int, portOffset: Int): String {
            return hostname + ":" + calculatePort(nodeId, portBase, portOffset)
        }
    }
}
