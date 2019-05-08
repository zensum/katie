package se.zensum.katie

import franz.JobStatus
import franz.ProducerBuilder
import franz.WorkerBuilder
import franz.producer.ProduceResult
import io.ktor.http.HttpStatusCode
import khttp.responses.Response
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import org.apache.kafka.common.errors.TimeoutException
import se.zensum.franzSentry.SentryInterceptor
import se.zensum.idempotenceconnector.IdempotenceStore
import se.zensum.webhook.PayloadOuterClass
import java.net.URL
import java.util.concurrent.CompletableFuture


fun getEnv(e: String, default: String? = null): String = System.getenv()[e] ?: default
    ?: throw RuntimeException("Missing environment variable $e and no default value is given.")

private val logger = KotlinLogging.logger("main")
private val routes: Map<String, Route> = readRoutes()
private val KAFKA_HOST: String = getEnv("KAFKA_HOST", "kafka")
private val idempotenceStore = IdempotenceStore(set = "katie")
private val producer = ProducerBuilder.ofByteArray.option("bootstrap.servers", KAFKA_HOST)
    .create()

fun main(args: Array<String>) = runBlocking {
    WorkerBuilder.ofByteArray
        .subscribedTo(routes.keys)
        .groupId("katie")
        .install(SentryInterceptor())
        .handlePiped {
            val topic: String = it.value?.topic() ?: return@handlePiped JobStatus.PermanentFailure
            val offset: Long = it.value?.offset() ?: return@handlePiped JobStatus.PermanentFailure
            val url: URL? = routes[topic]!!.url
            val responseTopic: String? = routes[topic]!!.responseTopic
            it
                .require("URL is not null for topic ($topic)") { url != null }
                .map { Payload(it.value()) }
                .advanceIf("Not in idempotence store") { checkIdempotenceStore(it, offset) }
                .execute("Send to $url") { send(url!!, it, responseTopic) }
                .execute("Write in idempotence store") { writeToIdempotenceStore(it, offset) }
                .end()
        }.start()
}

private suspend fun writeToIdempotenceStore(it: PayloadOuterClass.Payload, offset: Long): Boolean {
    return when (it.repeating) {
        true -> idempotenceStore.put("${it.flakeId}:$offset")
        false -> idempotenceStore.put(it.flakeId.toString())
    }
}

private suspend fun checkIdempotenceStore(it: PayloadOuterClass.Payload, offset: Long): Boolean {
    return when (it.repeating) {
        true -> !idempotenceStore.contains("${it.flakeId}:$offset")
        false -> !idempotenceStore.contains(it.flakeId.toString())
    }
}

fun Payload(bytes: ByteArray): PayloadOuterClass.Payload = PayloadOuterClass.Payload.parseFrom(bytes)

val defaultAcceptedRange: IntRange = 200..399

private suspend fun send(url: URL,
                         payload: PayloadOuterClass.Payload,
                         responseTopic: String?,
                         validCodes: Collection<Int> = emptyList(),
                         validCodesRange: IntRange = defaultAcceptedRange): Boolean {
    val response: Response = sendAsyncRequest(url, payload)
    responseTopic?.let {
        writeResponseToKafka(responseTopic, response, payload.flakeId)
    }
    return acceptCode(response.statusCode, validCodes, validCodesRange).also { accepted ->
        if (!accepted)
            logger.error("Got unexpected se.zensum.katie.createResponse ${response.statusCode} for request to $url with repeatingId ${payload.flakeId}")
    }
}

private suspend fun sendAsyncRequest(url: URL, p: PayloadOuterClass.Payload): Response {
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
    return requestResult.await()
}

private suspend fun writeResponseToKafka(topic: String, response: Response, id: Long): Int {
    val body: ByteArray = createResponse(response, id).toByteArray()
    return writeResponseToKafka(response.url, topic, body, response.statusCode)
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

private suspend fun writeResponseToKafka(path: String, topic: String, data: ByteArray, successResponse: Int): Int {
    return try {
        val metaData: ProduceResult = producer.send(topic, data)
        logger.info("$path written to ${metaData.topic()}")
        successResponse
    } catch (e: TimeoutException) {
        logger.error("Time out when trying to write $path to $topic")
        HttpStatusCode.InternalServerError.value
    }
}