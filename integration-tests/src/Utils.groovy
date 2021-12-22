@Grab(group = 'org.apache.commons', module = 'commons-exec', version = '1.3')

import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.Executor
import org.apache.commons.exec.PumpStreamHandler

class Utils {
    private static final RED = '\033[0;31m'
    private static final GREEN = '\033[0;32m'
    private static final YELLOW = '\033[0;33m'
    private static final MAGENTA = '\033[0;35m'
    private static final CYAN = '\033[0;36m'
    private static final NC = '\033[0m'

    /**
     * Run a command. If the command fails exit the program with the same
     * exit code.
     * @param command the command to run
     * @param workingDirectory the working directory (optional)
     * @param captureOutput true if the command's output should be returned
     * as a string, false if the output should be forwarded to stdout
     * @return the command's output or an empty string
     */
    static String run(command, File workingDirectory = null,
            boolean captureOutput = false, int retries = 0) {
        while (true) {
            logExec(command.toString())

            CommandLine cmdLine
            if (command instanceof String || command instanceof GString) {
                cmdLine = CommandLine.parse(command)
            } else {
                String[] carr = (String[])command
                cmdLine = new CommandLine(carr[0])
                cmdLine.addArguments(Arrays.copyOfRange(carr, 1, carr.length), false)
            }
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
                if (retries > 0) {
                    logWarn("Command '${command}' failed with exit code $exitValue")
                    logWarn("Retrying")
                    retries--
                    Thread.sleep(1000)
                    continue
                } else {
                    logFail("Command '${command}' failed with exit code $exitValue")
                    System.err.println(baos.toString())
                    System.exit(exitValue)
                }
            }

            if (captureOutput) {
                return baos.toString()
            }

            return ""
        }
    }

    /**
     * Wait for an HTTP server to become alive. Try for 180 seconds and
     * if the server still does not respond exit the program with exit
     * code 1.
     * @param url the URL the HTTP server listens to
     */
    static boolean waitHttp(String url, String method = 'HEAD',
            Integer expectedStatusCode = null) {
        logWait(url)

        int timeout = 180
        int count = 0
        for (int i = 0; i < timeout; ++i) {
            try {
                def c = new URL(url).openConnection()
                c.connectTimeout = 1000
                c.readTimeout = 1000
                c.requestMethod = method
                // c.responseCode will do the actual request
                int code = c.responseCode
                if (expectedStatusCode != null && code == expectedStatusCode) {
                    return true
                } else if (c.responseCode >= 200 && c.responseCode < 400) {
                    return true
                }
            } catch (IOException exception) {
                // connection was not successful. fall through.
            }
            Thread.sleep(1000)
            count++
            if (count % 20 == 0) {
                logWarn("$url not available within $count seconds. Waiting ${timeout - count} more seconds ...")
            }
        }
        fail("$url not available within $timeout seconds")
    }

    static void logExec(String msg) {
        println("[${CYAN}EXEC${NC}] $msg")
    }

    static void logWait(String msg) {
        println("[${CYAN}WAIT${NC}] $msg")
    }

    static void logWarn(String msg) {
        println("[${YELLOW}WARN${NC}] $msg")
    }

    static void logFail(String msg) {
        println("[${RED}FAIL${NC}] $msg")
    }

    static void logTest(String msg) {
        println("[${MAGENTA}TEST${NC}] $msg")
    }

    static void logOK(String msg) {
        println("[${GREEN} OK ${NC}] $msg")
    }

    static void logSuccess() {
        logOK("Success.")
    }

    static void fail(String msg) {
        logFail(msg)
        System.exit(1)
    }

    static void assertEquals(a, b, msg) {
        if (a != b) {
            fail(msg)
        }
    }

    static void assertTrue(b, msg) {
        if (!b) {
            fail(msg)
        }
    }
}
