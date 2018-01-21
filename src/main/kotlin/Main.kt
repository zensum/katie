package se.zensum.katie

import franz.JobStatus
import franz.ProducerBuilder
import franz.WorkerBuilder
import franz.producer.ProduceResult
import khttp.responses.Response
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.future.await
import mu.KotlinLogging
import org.apache.kafka.common.errors.TimeoutException
import org.jetbrains.ktor.http.HttpMethod
import org.jetbrains.ktor.http.HttpStatusCode
import org.jetbrains.ktor.request.ApplicationRequest
import org.jetbrains.ktor.request.receiveStream
import se.zensum.idempotenceconnector.IdempotenceStore
import se.zensum.webhook.PayloadOuterClass
import java.net.URL
import java.text.Format
import java.util.concurrent.CompletableFuture

fun getEnv(e: String, default: String? = null): String = System.getenv()[e] ?: default
?: throw RuntimeException("Missing environment variable $e and no default value is given.")

private val logger = KotlinLogging.logger("main")
private val routes: Map<String, URL> = readRoutes()
private val idempotenceStore = IdempotenceStore(set = "katie")
private val producer = ProducerBuilder.ofByteArray
    .option("client.id", "katie")
    .create()

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

val defaultAcceptedRange: IntRange = 200..399

private suspend fun send(url: URL,
                         payload: PayloadOuterClass.Payload,
                         validCodes: Collection<Int> = emptyList(),
                         validCodesRange: IntRange = defaultAcceptedRange): Boolean {
    val result: CompletableFuture<Int> = sendAsyncRequest(url, payload)
    val code: Int = result.await()
    return acceptCode(code, validCodes, validCodesRange).also { accepted ->
        if (!accepted)
            logger.error("Got unexpected response $code for request to $url with id ${payload.flakeId}")
    }
}

private suspend fun sendAsyncRequest(url: URL, p: PayloadOuterClass.Payload): CompletableFuture<Int> {
    val requestResult: CompletableFuture<Response> = CompletableFuture()
    khttp.async.request(
        method = p.method.name,
        headers = toMap(p.headers),
        url = url.toString(),
        params = toMap(p.parameters),
        data = p.body,
        onError = {
            logger.error("Request failed to $url with request ${p.flakeId}")
            requestResult.completeExceptionally(this)
        },
        onResponse = { requestResult.complete(this) },
        timeout = 45.0
    )
    val response: Response = requestResult.await()
    val kafkaResult: CompletableFuture<Int> = createResponse(response)
    val createResponse = createResponse(response)
    return requestResult
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

suspend fun createResponse(response: Response): Int {
    val method = response.request.method
    val path: String = response.url
    val body: ByteArray = when (routes.format) {
        Format.RAW_BODY -> receiveBody(response.request)
        Format.PROTOBUF -> createPayload(response).toByteArray()
    }

    return writeToKafka(method, path, "test", body, routing.response)
}

private fun hasBody(req: ApplicationRequest): Boolean = Integer.parseInt(req.headers["Content-Length"] ?: "0") > 0

private suspend fun receiveBody(req: ApplicationRequest): ByteArray = when (hasBody(req)) {
    true -> req.call.receiveStream().readBytes(64)
    false -> ByteArray(0)
}

private suspend fun writeToKafka(method: HttpMethod, path: String, topic: String, data: ByteArray, successResponse: HttpStatusCode): Int {
    val summary = "${method.value} $path"
    return try {
        val metaData: ProduceResult = producer.send(topic, data)
        logger.info("$summary written to ${metaData.topic()}")
        successResponse.value
    } catch (e: TimeoutException) {
        logger.error("Time out when trying to write $summary to $topic")
        HttpStatusCode.InternalServerError.value
    }
}