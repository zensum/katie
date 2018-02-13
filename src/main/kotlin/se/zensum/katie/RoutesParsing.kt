package se.zensum.katie

import com.moandjiezana.toml.Toml
import java.io.File
import java.net.URL

private const val ROUTES_FILE = "/etc/config/routes"

data class Route(val topic: String, val url: URL, val responseTopic: String?)

fun readRoutes(routesFile: String = getEnv("ROUTES_FILE", ROUTES_FILE)):Map<String, Route> {
    val toml = readTomlFromFile(routesFile)
    return parseTomlConfig(toml)
}

private fun readTomlFromFile(routesFile: String): Toml {
    val file: File = verifyFile(routesFile)
    return Toml().read(file)
}

fun parseTomlConfig(toml: Toml):  Map<String, Route> {
    val routes = toml.getTables("routes")
    return routes.asSequence()
        .map { parseEntry(it) }
        .toMap()
}

fun parseEntry(routeConfig: Toml): Pair<String, Route> {
    val topic: String = readOrDie(routeConfig, "topic")
    val url: URL = URL(readOrDie(routeConfig, "url"))
    val responseTopic: String? = routeConfig.getString("responseTopic")
    return Pair(topic, Route(topic, url, responseTopic))
}

private fun readOrDie(config: Toml, key: String): String {
    return config.getString(key) ?: throw IllegalArgumentException("Missing mandatory key \"$key\" in config")
}

fun verifyFile(filePath: String): File {
    return File(filePath).apply {
        if (!exists()) {
            throw IllegalArgumentException("File $this does not exist")
        }
        if (isDirectory) {
            throw IllegalArgumentException("File $this is a directory")
        }
    }
}