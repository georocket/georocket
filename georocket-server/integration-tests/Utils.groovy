@Grab(group = 'org.apache.commons', module = 'commons-exec', version = '1.3')

import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.Executor
import org.apache.commons.exec.PumpStreamHandler

class Utils {
    /**
     * Run a command. If the command fails exit the program with the same
     * exit code.
     * @param command the command to run
     * @param workingDirectory the working directory (optional)
     * @param captureOutput true if the command's output should be returned
     * as a string, false if the output should be forwarded to stdout
     * @return the command's output or an empty string
     */
    static String run(String command, File workingDirectory = null, captureOutput = false) {
        println("RUN  $command")

        CommandLine cmdLine = CommandLine.parse(command)
        Executor executor = new DefaultExecutor()

        if (workingDirectory != null) {
            executor.workingDirectory = workingDirectory
        }

        ByteArrayOutputStream baos = null
        if (captureOutput) {
            baos = new ByteArrayOutputStream()
            PumpStreamHandler psh = new PumpStreamHandler(baos)
            executor.setStreamHandler(psh)
        }

        // do not check exit code for us, we check it ourselves
        executor.setExitValues(null)

        int exitValue = executor.execute(cmdLine)
        if (exitValue != 0) {
            println("FAIL Command '${command}' failed with exit code $exitValue")
            System.exit(exitValue)
        }

        if (captureOutput) {
            return baos.toString()
        }

        return ""
    }

    /**
     * Wait for an HTTP server to become alive. Try for 60 seconds and
     * if the server still does not respond exit the program with exit
     * code 1.
     * @param url the URL the HTTP server listens to
     */
    static boolean waitHttp(String url) {
        println "WAIT $url"

        for (int i = 0; i < 60; ++i) {
            try {
                def c = new URL(url).openConnection()
                c.connectTimeout = 1
                c.readTimeout = 1
                c.requestMethod = 'HEAD'

                // TODO enable this as soon as GeoRocket supports HEAD on /
                // if (c.responseCode >= 200 && c.responseCode < 400) {
                //     return true
                // }
                c.responseCode // do the actual request now
                return true
            } catch (IOException exception) {
                // connection was not successful. fall through.
            }
            Thread.sleep(1000)
        }
        println "FAIL $url not available within 60 seconds"
        System.exit(1)
    }
}
