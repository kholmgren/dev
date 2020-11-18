package io.kettil.fn.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import picocli.CommandLine;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

@Slf4j
@Data
public class Sink implements Runnable {
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    String host = "localhost";

    @CommandLine.Option(names = {"-p", "--port"}, description = "liiklus port", defaultValue = "6565")
    private int port;

    @CommandLine.Parameters(paramLabel = "TOPIC", description = "liiklus topic", defaultValue = "fn-topic")
    String topic;

    @CommandLine.Parameters(paramLabel = "GROUP", description = "liiklus group", defaultValue = "fn-group")
    String group;

    public static void main(String[] args) {
        new CommandLine(new Sink()).execute(args);
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

        // Consume the events
        Function<Integer, Function<ReceiveReply.Record, Publisher<?>>> businessLogic = partition -> record -> {
            log.info("Processing record from partition {} offset {}", partition, record.getOffset());

            // simulate processing
            return Mono.delay(Duration.ofMillis(200));
        };

        stub
                .subscribe(subscribeAction)
                .filter(it -> it.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT)
                .map(SubscribeReply::getAssignment)
                .doOnNext(assignment -> log.info("Assigned to partition {}", assignment.getPartition()))
                .flatMap(assignment -> stub
                        // Start receiving the events from a partition
                        .receive(ReceiveRequest.newBuilder().setAssignment(assignment).build())
                        .window(2) // ACK every nth record
                        .concatMap(
                                batch -> batch
                                        .map(ReceiveReply::getRecord)
                                        .delayUntil(businessLogic.apply(assignment.getPartition()))
                                        .sample(Duration.ofSeconds(5)) // ACK every 5 seconds
                                        .onBackpressureLatest()
                                        .delayUntil(record -> {
                                            log.info("ACKing partition {} offset {}", assignment.getPartition(), record.getOffset());
                                            return stub.ack(
                                                    AckRequest.newBuilder()
                                                            .setTopic(subscribeAction.getTopic())
                                                            .setGroup(subscribeAction.getGroup())
                                                            .setGroupVersion(subscribeAction.getGroupVersion())
                                                            .setPartition(assignment.getPartition())
                                                            .setOffset(record.getOffset())
                                                            .build()
                                            );
                                        }),
                                1
                        )
                )
                .blockLast();

        log.info("complete");

        latch.await();
    }
}
