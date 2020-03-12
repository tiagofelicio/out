create or replace and compile java source named out."OUTTools" as
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class OUTTools {

    public static final String[] ENVIRONMENT = { "PATH=/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin" };

    public static final String SHELL_OUTPUT_SEPARATOR() {
        return "~";
    }

    public static String shell(String command) {
        Runtime runtime = Runtime.getRuntime();
        String[] commands = { "sh", "-c", command };
        Process process = null;
        BufferedReader reader = null;
        String line = null;
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        int exitValue = 999;
        try {
            process = runtime.exec(commands, ENVIRONMENT);
            exitValue = process.waitFor();
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((line = reader.readLine()) != null)
                stdout.append(line + System.lineSeparator());
            if (stdout.length() != 0)
                stdout.delete(stdout.length() - System.lineSeparator().length(), stdout.length());
            reader.close();
            reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while ((line = reader.readLine()) != null)
                stderr.append(line + System.lineSeparator());
            if (stderr.length() != 0)
                stderr.delete(stderr.length() - System.lineSeparator().length(), stderr.length());
        } catch (IOException | InterruptedException e) {
        } finally {
            try {
                reader.close();
            } catch (Exception e) { }
        }
        return exitValue + SHELL_OUTPUT_SEPARATOR() + stdout.toString() + SHELL_OUTPUT_SEPARATOR() + stderr.toString();
    }

};
/