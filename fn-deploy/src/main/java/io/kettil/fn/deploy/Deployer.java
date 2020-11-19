package io.kettil.fn.deploy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.kettil.fn.deploy.model.Manifest;
import io.kettil.fn.deploy.model.ManifestRuntimes;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
public class Deployer implements Callable<Integer> {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @CommandLine.Option(names = {"-i", "--invoker-jar"}, description = "invoker jar file", defaultValue = "../fn-invoker/target/fn-invoker-1.0.jar")
    private File invokerJar;


    @CommandLine.Parameters(paramLabel = "DIR", description = "function repo dir")
    String dir;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Deployer()).execute(args);
        System.exit(exitCode);
    }

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Integer call() throws Exception {
        Path manifestPath = Paths.get(dir, "manifest.yaml");
        if (Files.notExists(manifestPath)) {
            System.err.println("Manifest file does not exist: " + manifestPath);
            return 1;
        }

        if (!invokerJar.exists()) {
            System.err.println("Invoker jar file does not exist: " + invokerJar);
            return 1;
        }

        System.out.println("Using manifest file: " + manifestPath);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();

        Manifest manifest = mapper.readValue(manifestPath.toFile(), Manifest.class);
        Path location = manifestPath.getParent().resolve(manifest.getLocation().toPath());

        if (Files.notExists(location)) {
            System.err.println("Location path in manifest does not exist: " + manifest.getLocation());
            return 1;
        }

        ManifestRuntimes rt = Arrays.stream(ManifestRuntimes.values())
                .filter(i -> i.name().compareToIgnoreCase(manifest.getRuntime()) == 0)
                .findFirst()
                .orElse(null);

        if (rt == null) {
            System.err.println("Unsupported runtime: " + manifest.getRuntime());
            return 1;
        }

        System.out.println("Using runtime " + rt + " and artefact: " + location);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            latch.countDown();
        }));

        Path sourceInvokerJar = invokerJar.toPath();
        String invokerJar = "fn-invoker.jar";

        Path sourceFunctionJar = location.toAbsolutePath().normalize();
        String functionJar = "fn.jar";

        Map<String, String> env = new LinkedHashMap<>();
        env.put("FN_NAME", manifest.getName());
        env.put("FN_LOCATION", functionJar);

        DockerfileBuilder dockerfileBuilder = new DockerfileBuilder();
        String dockerFile = dockerfileBuilder
                .from("openjdk:8-jdk-alpine")
                .copy(invokerJar, invokerJar)
                .copy(functionJar, functionJar)
                .env(env)
                .cmd("java", "-jar", invokerJar)
                .build();

        System.out.println("Using Dockerfile:");
        System.out.println(dockerFile.replaceAll("(?m)^", "\t"));

        ImageFromDockerfile image = new ImageFromDockerfile()
                .withFileFromString("Dockerfile", dockerFile)
                .withFileFromPath(invokerJar, sourceInvokerJar)
                .withFileFromPath(functionJar, sourceFunctionJar);

        try (GenericContainer container = new GenericContainer(image)) {
            log.info("Starting container");

            container.start();

            Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
            container.followOutput(logConsumer);

            while (!latch.await(1000, TimeUnit.MILLISECONDS)) {
                if (container.getContainerInfo().getState().getDead())
                    break;
            }
        }

        return 0;
    }
}
