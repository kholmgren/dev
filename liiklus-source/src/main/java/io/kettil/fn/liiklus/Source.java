package io.kettil.fn.liiklus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
@CommandLine.Command(
        name = "liiklus-source",
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "generates events to liiklus")
public class Source implements Runnable {
    @Option(names = {"--count"}, description = "Number of items to simulate")
    private int count = 20;

    @Option(names = {"--minRate"}, description = "Shortest delay between events in seconds")
    private int minRate = 1;

    @Option(names = {"--maxRate"}, description = "Longest delay between events in seconds")
    private int maxRate = 5;

    @Option(names = {"--minLimit"}, description = "Shortest delay between events in seconds")
    private int minLimit = 0;

    @Option(names = {"--maxLimit"}, description = "Longest delay between events in seconds")
    private int maxLimit = 100;

    @CommandLine.Option(names = {"--host"}, description = "liiklus host", defaultValue = "localhost")
    private String liiklusHost;

    @CommandLine.Option(names = {"--port"}, description = "liiklus port", defaultValue = "6565")
    private int liiklusPort;

    @Parameters(paramLabel = "TOPIC", description = "liiklus topic", defaultValue = "fn-output-topic")
    String topic;

    private Random random = new Random();
    private ObjectMapper mapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());

    private final PriorityQueue<FnEvent> pendingUpdates = new PriorityQueue<>(
            Comparator.comparing(FnEvent::getNextUpdate));

    private LocalDateTime next(LocalDateTime now) {
        return now
                .plusSeconds(random.nextInt(maxRate - minRate) + minRate)
                .plusNanos((long) (random.nextDouble() * 1e9));
    }

    private int makeNextValue(int current) {
        int upOrDownOrNot = random.nextInt(3) - 1;
        int v = current + upOrDownOrNot;

        v = Math.max(minLimit, v);
        v = Math.min(maxLimit, v);

        return v;
    }

    private void initPendingUpdates() {
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < count; i++) {
            int value = random.nextInt(maxLimit - minLimit) + minLimit;
            FnEvent s = new FnEvent(
                    "item:" + i,
                    now,
                    value,
                    next(now));

            pendingUpdates.add(s);
        }
    }

    public static void main(String[] args) {
        new CommandLine(new Source()).execute(args);
    }

    private final CountDownLatch latch = new CountDownLatch(1);

    @SneakyThrows
    public void run() {
        initPendingUpdates();

        ManagedChannel channel = NettyChannelBuilder.forTarget(liiklusHost + ":" + liiklusPort)
                .directExecutor()
                .usePlaintext()
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");

            channel.shutdown();

            try {
                channel.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Already in shutdown hook--don't call Thread.currentThread().interrupt()
            } finally {
                channel.shutdownNow();
            }

            latch.countDown();
        }));

        log.info("opened channel");

        ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

        log.info("created service stub");

        while (!latch.await(100, TimeUnit.MILLISECONDS)) {
            LocalDateTime now = LocalDateTime.now();
            FnEvent s = pendingUpdates.poll();

            if (s.getNextUpdate().isAfter(now)) {
                pendingUpdates.add(s);
                Thread.sleep(100);
                continue;
            }

            FnEvent fnevt = new FnEvent(
                    s.getId(),
                    now,
                    makeNextValue(s.getValue()),
                    next(now));

            pendingUpdates.add(fnevt);

            System.out.println(mapper.writeValueAsString(fnevt));

            LiiklusEvent liiklusEvent = LiiklusEvent.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setType("io.kettil.fn.event")
                    .setSource("/example")
                    .setDataContentType("application/json")
                    .setData(ByteString.copyFrom(mapper.writeValueAsBytes(fnevt)))
                    .build();

            PublishRequest publishRequest = PublishRequest.newBuilder()
                    .setTopic(topic)
                    .setKey(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                    .setLiiklusEvent(liiklusEvent)
                    .build();

            stub
                    .publish(publishRequest)
                    .block(Duration.of(1000, ChronoUnit.MILLIS));

        }
    }
}
