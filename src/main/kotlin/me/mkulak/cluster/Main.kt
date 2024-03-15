package me.mkulak.cluster

import io.aeron.cluster.ClusteredMediaDriver
import io.aeron.cluster.client.AeronCluster
import io.aeron.cluster.service.ClusteredServiceContainer
import io.aeron.driver.MediaDriver
import io.aeron.driver.ThreadingMode
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import org.agrona.ErrorHandler
import org.agrona.concurrent.ShutdownSignalBarrier
import kotlin.concurrent.thread
import kotlin.time.Duration.Companion.seconds

fun main() = runBlocking {
    val config = loadConfig()

    val clusterConfig = ClusterConfig.create(
        config.nodeId,
        config.hostnames,
        config.internalHostnames,
        config.portBase,
        CounterService()
    )
    clusterConfig.errorHandler(errorHandler(""))

    val mediaDriver = ClusteredMediaDriver.launch(
        clusterConfig.mediaDriverContext(),
        clusterConfig.archiveContext(),
        clusterConfig.consensusModuleContext()
    )
    val barrier = ShutdownSignalBarrier()
    Runtime.getRuntime().addShutdownHook(thread(start = false) {
        println("shutting down")
        barrier.signal()
    })
    mediaDriver.use {
        val serviceContainer = ClusteredServiceContainer.launch(clusterConfig.clusteredServiceContext())
        serviceContainer.use {
            println("[${config.nodeId}] Started Cluster Node on ${config.hostnames[config.nodeId]}...")
            startHttpServer(config)
            barrier.await()
            println("[${config.nodeId}] Exiting")
        }
    }
}

private suspend fun startHttpServer(config: AppConfig) {
    println("Creating cluster client")
    val clusterClient = createCounterClient()
    println("Starting http server")
    val vertx = Vertx.vertx()
    val router = Router.router(vertx)
    router.get("/").handler(vertx) {
        it.response().end("ok")
    }
    router.get("/counter").handler(vertx) {
        val value = clusterClient.get()
        it.response().end(value.toString())
    }
    router.patch("/counter").handler(vertx) {
        val delta = it.request().getParam("delta").toLong()
        println("/patch delta=$delta")
        val value = clusterClient.add(delta)
        println("result = $value")
        it.response().end(value.toString())
    }
    vertx.createHttpServer().requestHandler(router).listen(config.httpPort).coAwait()
    println("Http server started at locahost:${config.httpPort}")
}

suspend fun createCounterClient(): CounterClient {
    val hostnames = System.getProperty("aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",")
    val ingressEndpoints = ingressEndpoints(hostnames)

    val client = CounterClientImpl()
    retry(50, 1.seconds) {
        println("Cluster client connecting $it")

        val mediaDriverCtx = MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)

        val mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx)
        val clusterCtx = AeronCluster.Context()
            .egressListener(client)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .ingressChannel("aeron:udp")
            .ingressEndpoints(ingressEndpoints)
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
        client.start(clusterCtx)
    }
    return client
}

fun errorHandler(context: String): ErrorHandler = ErrorHandler { t: Throwable ->
    System.err.println(context)
    t.printStackTrace(System.err)
}

fun loadConfig(): AppConfig = AppConfig(
    nodeId = env("NODE_ID").toInt(),
    portBase = env("PORT_BASE", "9000").toInt(),
    httpPort = env("HTTP_PORT", "8080").toInt(),
    hostnames = env("HOSTNAMES", "localhost,localhost,localhost").split(","),
    internalHostnames = env("INTERNAL_HOSTNAMES", "localhost,localhost,localhost").split(",")
)

data class AppConfig(
    val nodeId: Int,
    val portBase: Int,
    val httpPort: Int,
    val hostnames: List<String>,
    val internalHostnames: List<String>
)

fun ingressEndpoints(hostnames: List<String?>): String =
    hostnames.mapIndexed { i, name -> i to name }.joinToString(",") { (i, name) -> "$i=$name:${calculatePort(i)}" }

private const val PORT_BASE = 9000
private const val PORTS_PER_NODE = 100
private const val CLIENT_FACING_PORT_OFFSET: Int = 2

fun calculatePort(nodeId: Int): Int =
    PORT_BASE + (nodeId * PORTS_PER_NODE) + CLIENT_FACING_PORT_OFFSET

