package se.zensum.katie

import franz.JobStatus
import franz.WorkerBuilder
import se.zensum.webhook.PayloadOuterClass
import java.net.URL

fun getEnv(e : String, default: String? = null) : String = System.getenv()[e] ?: default ?: throw RuntimeException("Missing environment variable $e and no default value is given.")

private val routes: Map<String, URL> = readRoutes()

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
                .advanceIf("Not in idempotence store") { false }
                .execute("Send to $url") { send(url!!, it) }
                .execute("Write in idempotence store") { false }
                .end()
        }.start()
}

fun Payload(bytes: ByteArray): PayloadOuterClass.Payload = PayloadOuterClass.Payload.parseFrom(bytes)

private suspend fun send(url: URL, p: PayloadOuterClass.Payload): Boolean {
    khttp.async.request(
        method = p.method.name,
        headers = toMap(p.headers),
        url = url.toString(),
        params = TODO("p.parameters"),
        data = p.body,
        onError = TODO(),
        onResponse = TODO()
    )
}

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