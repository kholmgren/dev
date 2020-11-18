package io.kettil.fn.liiklus;

import lombok.Data;
import picocli.CommandLine;

@Data
public class Sink implements Runnable {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @CommandLine.Option(names = {"-p", "--port"}, description = "liiklus port", defaultValue = "6565")
    private int port;

    @CommandLine.Parameters(paramLabel = "TOPIC", description = "liiklus topic", defaultValue = "fn-topic")
    String topic;

    @CommandLine.Parameters(paramLabel = "GROUP", description = "liiklus group", defaultValue = "fn-group")
    String group;

    public static void main(String[] args) {
        new CommandLine(new Sink()).execute(args);
    }

    public void run() {
        System.out.println(this.getTopic());
        System.out.println("XXXX: " + this);
    }
}
