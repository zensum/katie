package se.zensum.katie

import com.moandjiezana.toml.Toml
import java.io.File
import java.net.URL

private const val ROUTES_FILE ="/etc/config/routes"

fun readRoutes(routesFile: String = getEnv("ROUTES_FILE", ROUTES_FILE)): Map<String, URL> {
    val toml = readTomlFromFile(routesFile)
    return parseTomlConfig(toml)
}

private fun readTomlFromFile(routesFile: String): Toml
{
    val file: File = verifyFile(routesFile)
    return Toml().read(file)
}

fun parseTomlConfig(toml: Toml): Map<String, URL> {
    val routes = toml.getTables("routes")
    return routes.asSequence()
        .map { parseEntry(it) }
        .toMap()
}

fun parseEntry(routeConfig: Toml): Pair<String, URL> {
    val topic: String = routeConfig.getString("topic")!!
    val url: URL = URL(routeConfig.getString("url")!!)
    return topic to url
}

fun verifyFile(filePath: String): File
{
    return File(filePath).apply {
        if(!exists()) {
            throw IllegalArgumentException("File $this does not exist")
        }
        if(isDirectory) {
            throw IllegalArgumentException("File $this is a directory")
        }
    }
}