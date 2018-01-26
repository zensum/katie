package se.zensum.katie

import franz.JobStatus
import franz.ProducerBuilder
import franz.WorkerBuilder
import franz.producer.ProduceResult
import khttp.responses.Response
import kotlinx.coroutines.experimental.future.await
import mu.KotlinLogging
import org.apache.kafka.common.errors.TimeoutException
import org.jetbrains.ktor.http.HttpStatusCode
import se.zensum.idempotenceconnector.IdempotenceStore
import se.zensum.webhook.PayloadOuterClass
import java.net.URL
import java.util.concurrent.CompletableFuture


fun getEnv(e: String, default: String? = null): String = System.getenv()[e] ?: default
    ?: throw RuntimeException("Missing environment variable $e and no default value is given.")

private val logger = KotlinLogging.logger("main")
private val routes: Map<String, URL> = readRoutes()
private val KAFKA_HOST: String = getEnv("KAFKA_HOST", "kafka")
private val idempotenceStore = IdempotenceStore(set = "katie")
private val producer = ProducerBuilder.ofByteArray.option("bootstrap.servers", KAFKA_HOST)
    .create().forTopic("test")

fun main(args: Array<String>) {
    WorkerBuilder.ofByteArray
        .subscribedTo("test")
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
    val result: Int = sendAsyncRequest(url, payload)
    return acceptCode(result, validCodes, validCodesRange).also { accepted ->
        if (!accepted)
            logger.error("Got unexpected se.zensum.katie.createResponse $result for request to $url with id ${payload.flakeId}")
    }
}

private suspend fun sendAsyncRequest(url: URL, p: PayloadOuterClass.Payload): Int {
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
    return when (response.statusCode in defaultAcceptedRange) {
        true -> writeToKafka(response, p.flakeId)
        false -> response.statusCode
    }
}

private suspend fun writeToKafka(response: Response, id: Long): Int {
    val body: ByteArray = createResponse(response, id).toByteArray()
    return writeToKafka(response.url, "test", body, response.statusCode)
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

private suspend fun writeToKafka(path: String, topic: String, data: ByteArray, successResponse: Int): Int {
    return try {
        val metaData: ProduceResult = producer.send(topic, data)
        logger.info("$path written to ${metaData.topic()}")
        successResponse
    } catch (e: TimeoutException) {
        logger.error("Time out when trying to write $path to $topic")
        HttpStatusCode.InternalServerError.value
    }
}