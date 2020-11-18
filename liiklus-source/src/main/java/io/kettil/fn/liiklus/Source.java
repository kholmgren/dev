package io.kettil.fn.liiklus;

import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Data
public class Source implements Runnable {
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    private String host = "localhost";

    @Option(names = {"-p", "--port"}, description = "liiklus port", defaultValue = "6565")
    private int port;

    @Parameters(paramLabel = "TOPIC", description = "liiklus topic", defaultValue = "fn-topic")
    String topic;

    @Parameters(paramLabel = "GROUP", description = "liiklus group", defaultValue = "fn-group")
    String group;

    public static void main(String[] args) {
        new CommandLine(new Source()).execute(args);
    }

    private final CountDownLatch latch = new CountDownLatch(1);

    @SneakyThrows
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            latch.countDown();
        }));

        ManagedChannel channel = NettyChannelBuilder.forTarget(host + ":" + port)
                .directExecutor()
                .usePlaintext()
                .build();

        log.info("opened channel");

        SubscribeRequest subscribeAction = SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup(group)
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

        log.info("created service stub");

        Flux.interval(Duration.ofSeconds(1))
                .onBackpressureDrop()
                .concatMap(it -> stub.publish(
                        PublishRequest.newBuilder()
                                .setTopic(subscribeAction.getTopic())
                                .setKey(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                .setLiiklusEvent(
                                        LiiklusEvent.newBuilder()
                                                .setId(UUID.randomUUID().toString())
                                                .setType("com.example.event")
                                                .setSource("/example")
                                                .setDataContentType("text/plain")
                                                .setData(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                )
                                .build()
                ))
                .subscribe();

        log.info("subscribed to topic in group");

        latch.await();
    }
}
