package io.kettil.fn.liiklus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.bsideup.liiklus.protocol.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
@CommandLine.Command(
        name = "liiklus-sink",
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "displays events from liiklus")
public class Sink implements Runnable {
    @CommandLine.Option(names = {"--host"}, description = "liiklus host", defaultValue = "localhost")
    private String liiklusHost;

    @CommandLine.Option(names = {"--port"}, description = "liiklus port", defaultValue = "6565")
    private int liiklusPort;

    @CommandLine.Parameters(paramLabel = "TOPIC", description = "liiklus topic", defaultValue = "fn-input-topic")
    String topic;

    @CommandLine.Parameters(paramLabel = "GROUP", description = "liiklus group", defaultValue = "fn-input-group")
    String group;

    private ObjectMapper mapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());

    public static void main(String[] args) {
        new CommandLine(new Sink()).execute(args);
    }

    private final CountDownLatch latch = new CountDownLatch(1);

    @SneakyThrows
    public void run() {
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

        SubscribeRequest inSubscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup(group)
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
                .build();

        stub.subscribe(inSubscribeRequest)
                .filter(it -> it.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT)
                .map(SubscribeReply::getAssignment)
                .doOnNext(assignment -> log.info("Assigned to partition {}", assignment.getPartition()))
                .flatMap(assignment -> stub
                        .receive(ReceiveRequest.newBuilder().setAssignment(assignment).setFormat(ReceiveRequest.ContentFormat.LIIKLUS_EVENT).build())
                        .map(ReceiveReply::getLiiklusEventRecord)
                        .doOnNext(record -> {
                            try {
                                Object value = mapper.readValue(record.getEvent().getData().toByteArray(), Object.class);
                                System.out.println(value);

                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                log.debug("ACKing partition {} offset {}", assignment.getPartition(), record.getOffset());
                                stub.ack(
                                        AckRequest.newBuilder()
                                                .setTopic(inSubscribeRequest.getTopic())
                                                .setGroup(inSubscribeRequest.getGroup())
                                                .setGroupVersion(inSubscribeRequest.getGroupVersion())
                                                .setPartition(assignment.getPartition())
                                                .setOffset(record.getOffset())
                                                .build()
                                );
                            }
                        })
                )
                .doOnTerminate(latch::countDown)
                .subscribe();

        log.info("running");

        latch.await();
    }
}
