package me.mkulak.cluster

import io.vertx.core.Vertx
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.time.Duration

fun env(name: String, defaultValue: String? = null): String =
    System.getenv(name) ?: defaultValue ?: throw RuntimeException("Env var $name not set")

suspend fun <T> retry(maxTries: Int, backoff: Duration, f: suspend (Int) -> T): T {
    repeat(maxTries - 1) {
        runCatching { return f(it) }
        delay(backoff)
    }
    return f(maxTries - 1)
}

fun Route.handler(vertx: Vertx, f: suspend (RoutingContext) -> Unit): Route =
    handler {
        CoroutineScope(vertx.dispatcher()).launch {
            f(it)
        }
    }
