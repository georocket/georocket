package io.georocket

import java.net.ServerSocket
import java.io.IOException
import java.lang.RuntimeException

/**
 * Helper class for server tests.
 * @author Benedikt Hiemenz
 */
object NetUtils {
    /**
     * Find a free socket port.
     * @return the number of the free port
     */
    fun findPort(): Int {
        try {
            ServerSocket(0).use { socket -> return socket.localPort }
        } catch (e: IOException) {
            throw RuntimeException("Could not find a free port")
        }
    }
}
