package se.zensum.katie

import com.google.protobuf.ByteString
import khttp.responses.Response
import khttp.structures.cookie.CookieJar
import se.zensum.webhook.PayloadOuterClass
import se.zensum.webhook.ResponseOuterClass

fun createResponse(response: Response, requestId: Long ): ResponseOuterClass.Response {
    return response.run {
        val requestHeaders: Map<String, String> = headers
        val content: ByteArray = content
        val cookies: CookieJar = cookies
        val statusCode: Int = statusCode
        ResponseOuterClass.Response.newBuilder().apply {
            this.path = url
            this.content = ByteString.copyFrom(content)
            this.statusCode = statusCode
            this.headers = parseMap(requestHeaders)
            this.cookies = parseCookieJar(cookies)
            this.flakeId = requestId.also {
                if (it == 0L) throw IllegalStateException("Generated flake id was 0")
            }
        }.build()
    }
}

fun parseCookieJar(cookieJar: CookieJar): PayloadOuterClass.MultiMap {
    return PayloadOuterClass.MultiMap.newBuilder().apply {
        cookieJar.entries.asSequence()
            .map { it.toPair() }
            .map { toProtoPair(it) }
            .forEach { addPair(it) }
    }.build()
}

fun parseMap(valuesMap: Map<String, String>): PayloadOuterClass.MultiMap {
    return PayloadOuterClass.MultiMap.newBuilder().apply {
        valuesMap.entries.asSequence()
            .map { it.toPair() }
            .map { toProtoPair(it) }
            .forEach { addPair(it) }
    }.build()
}

fun toProtoPair(pair: Pair<String, String>): PayloadOuterClass.MultiMap.Pair {
    return PayloadOuterClass.MultiMap.Pair.newBuilder().apply {
        this.key = pair.first
        this.value = pair.second
    }.build()
}