package io.kettil.fn.liiklus;

import lombok.Data;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Data
public class Source implements Runnable {
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @Option(names = {"-p", "--port"}, description = "liiklus port", defaultValue = "6565")
    private int port;

    @Parameters(paramLabel = "TOPIC", description = "liiklus topic", defaultValue = "fn-topic")
    String topic;

    @Parameters(paramLabel = "GROUP", description = "liiklus group", defaultValue = "fn-group")
    String group;

    public static void main(String[] args) {
        new CommandLine(new Source()).execute(args);
    }

    public void run() {
        System.out.println(this.getTopic());
        System.out.println("XXXX: " + this);
    }
}
