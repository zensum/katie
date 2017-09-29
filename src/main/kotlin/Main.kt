package se.zensum.katie

import franz.JobStatus
import franz.WorkerBuilder
import kotlinx.coroutines.experimental.future.await
import mu.KotlinLogging
import se.zensum.idempotenceconnector.IdempotenceStore
import se.zensum.webhook.PayloadOuterClass
import java.net.URL
import java.util.concurrent.CompletableFuture

fun getEnv(e : String, default: String? = null) : String = System.getenv()[e] ?: default ?: throw RuntimeException("Missing environment variable $e and no default value is given.")

private val log = KotlinLogging.logger("main")
private val routes: Map<String, URL> = readRoutes()
private val idempotenceStore = IdempotenceStore("idempotence-store")

fun main(args: Array<String>) {
    WorkerBuilder.ofByteArray
        .subscribedTo(routes.keys)
        .groupId("katie")
        .handlePiped {
            val topic: String = it.value?.topic() ?: return@handlePiped JobStatus.PermanentFailure
            val url: URL? = routes[topic]
            it
                .require("URL is not null for topic ($topic)") { url != null }
                .map { Payload(it.value()) }
                .advanceIf("Not in idempotence store") { !idempotenceStore.contains(it.flakeId.toString()) }
                .execute("Send to $url") { send(url!!, it) }
                .execute("Write in idempotence store") { idempotenceStore.put(it.flakeId.toString()) }
                .end()
        }.start()
}

fun Payload(bytes: ByteArray): PayloadOuterClass.Payload = PayloadOuterClass.Payload.parseFrom(bytes)

val defaultAcceptedRange: IntRange = 200..400

private suspend fun send(url: URL,
                         payload: PayloadOuterClass.Payload,
                         validCodes: Collection<Int> = emptyList(),
                         validCodesRange: IntRange = defaultAcceptedRange): Boolean {
    val result: CompletableFuture<Int> = sendAsyncRequest(url, payload)
    val code: Int = result.await()
    return acceptCode(code, validCodes, validCodesRange).also { accepted ->
        if(!accepted)
            log.error("Got unexpected response $code for request to $url with id ${payload.flakeId}")
    }
}

private suspend fun sendAsyncRequest(url: URL, p: PayloadOuterClass.Payload): CompletableFuture<Int> {
    val result: CompletableFuture<Int> = CompletableFuture()
    khttp.async.request(
        method = p.method.name,
        headers = toMap(p.headers),
        url = url.toString(),
        params = toMap(p.parameters),
        data = p.body,
        onError = {
            log.error("Request failed to $url with request ${p.flakeId}")
            result.completeExceptionally(this)
        },
        onResponse = { result.complete(this.statusCode) },
        timeout = 45.0
    )
    return result
}

fun acceptCode(code: Int, coll: Collection<Int>, range: IntRange): Boolean = code in coll || code in range

fun toMap(headers: PayloadOuterClass.MultiMap): Map<String, String> {
    return headers.pairList.asSequence()
        .groupBy { it.key }
        .map { it.key to merge(it.value) }
        .toMap()
}

private fun merge(pairs: List<PayloadOuterClass.MultiMap.Pair>): String {
    return pairs.asSequence()
        .map { it.value }
        .joinToString(separator = ", ")
}