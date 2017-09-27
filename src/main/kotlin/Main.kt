package se.zensum.katie

import franz.WorkerBuilder
import franz.asPipe
import se.zensum.webhook.PayloadOuterClass
import java.net.URL

fun getEnv(e : String, default: String? = null) : String = System.getenv()[e] ?: default ?: throw RuntimeException("Missing environment variable $e and no default value is given.")

private val routes: Map<String, URL> = readRoutes()

fun main(args: Array<String>) {
    WorkerBuilder.ofByteArray
        .subscribedTo(routes.keys)
        .groupId("katie")
        .running {
            asPipe()
                .map(::Payload)
                .end()
            TODO("We can not read topic yet. We need to use a more recent version of Franz.")
        }.start()
}

private fun <T> WorkerBuilder<T>.subscribedTo(topics: Collection<String>): WorkerBuilder<T> {
    val topicArray = topics.toTypedArray()
    return merge(this, topicArray)
}

private tailrec fun <T> merge(workerBuilder: WorkerBuilder<T>, topics: Array<String>, i: Int = 0): WorkerBuilder<T> {
    return merge(workerBuilder.subscribedTo(topics[i]), topics, i+1)
}

fun Payload(bytes: ByteArray): PayloadOuterClass.Payload = PayloadOuterClass.Payload.parseFrom(bytes)