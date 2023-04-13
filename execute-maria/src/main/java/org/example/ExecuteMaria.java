package org.example;

import org.apache.commons.lang3.ArrayUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ExecuteMaria {
    public static void main(String[] args) throws IOException {
        Path mariaCmd = Paths.get(System.getenv("MARIA_HOME"), "bin/mysql.exe");
        String admin = args[0];
        String adminPassword = args[1];
        Runtime rt = Runtime.getRuntime();

        String[] commandPrev = {"\"" + mariaCmd + "\" " +
                "--user=" + admin + " --password=" + adminPassword + " "};
        String[] commandArray = ArrayUtils.addAll(commandPrev, ArrayUtils.subarray(args, 2, args.length+1));

        String command = Arrays.stream(commandArray).collect(Collectors.joining());

        Process proc = rt.exec(command);

        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream(), "MS949"));

        BufferedReader stdError = new BufferedReader(new
                InputStreamReader(proc.getErrorStream(), "MS949"));

        // Read the output from the command
        System.out.println("Here is the standard output of the command:\n");
        String s = null;
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
        }

        // Read any errors from the attempted command
        System.out.println("Here is the standard error of the command (if any):\n");
        while ((s = stdError.readLine()) != null) {
            System.out.println(s);
        }
    }
}
