package io.kettil.fn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Service
@Slf4j
public class FnStreamingController implements CommandLineRunner {
    @Autowired
    private FnProperties fnProperties;

    @Autowired
    private FunctionCatalog catalog;

    @Autowired
    private FunctionProperties functionProperties;

    @Autowired
    private ObjectMapper mapper;

    @Value("${liiklus.host}")
    private String liiklusHost;

    @Value("${liiklus.port}")
    private int liiklusPort;

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void run(String... args) throws Exception {
        System.out.println("fnProperties=" + fnProperties);

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
                .setTopic(fnProperties.getInTopic())
                .setGroup(fnProperties.getInGroup())
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        stub.subscribe(inSubscribeRequest)
                .filter(it -> it.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT)
                .map(SubscribeReply::getAssignment)
                .doOnNext(assignment -> log.info("Assigned to partition {}", assignment.getPartition()))
                .flatMap(assignment -> stub
                        .receive(ReceiveRequest.newBuilder().setAssignment(assignment).build())
                        .map(ReceiveReply::getRecord)
                        .doOnNext(record -> {
                            System.out.println("processing record=" + record);

                            try {
                                Object value = mapper.readValue(record.getValue().toByteArray(), Object.class);
                                Function<Object, Object> fn = catalog.lookup(functionProperties.getDefinition());

                                Object result = fn.apply(value);

                                stub.publish(
                                        PublishRequest.newBuilder()
                                                .setTopic(fnProperties.getOutTopic())
                                                .setKey(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                                .setLiiklusEvent(
                                                        LiiklusEvent.newBuilder()
                                                                .setId(UUID.randomUUID().toString())
                                                                .setType("io.kettil.fn.event")
                                                                .setSource("/example")
                                                                .setDataContentType("application/json")
                                                                .setData(ByteString.copyFrom(mapper.writeValueAsBytes(result)))
                                                )
                                                .build())
                                        .block(Duration.of(1000, ChronoUnit.MILLIS));

                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                log.info("ACKing partition {} offset {}", assignment.getPartition(), record.getOffset());
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
