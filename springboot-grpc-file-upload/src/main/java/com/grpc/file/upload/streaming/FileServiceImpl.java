package com.grpc.file.upload.streaming;


import com.grpc.file.upload.proto.ncclient.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Value;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//@GRpcService
@GrpcService
@Slf4j
public class FileServiceImpl extends FileUploadServiceGrpc.FileUploadServiceImplBase {

    private final String workingDirectory;

    public FileServiceImpl(@Value("${files.working.directory}") String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    @Override
    public StreamObserver<PublishMessageRequest> publishMessage(final StreamObserver<MessageResponse> responseObserver) {
        return new StreamObserver<>() {
            final Map<String, BufferedOutputStream> outputStreams = new HashMap<>();

            @Override
            public void onNext(PublishMessageRequest request) {

                log.info("\nprocessing data: ["  +"," + request.getSourceId() + "," + request.getMessage() + "]\n");

                List<Attachment> attachments = request.getAttachmentsList();

                log.info("Number of attachments received :: {}", request.getAttachmentsCount());

                for (Attachment att : attachments) {
                    byte[] data = att.getAttachmentBytes().toByteArray();
                    String name = att.getAttachmentName();

                    try {
                        if (!outputStreams.containsKey(name)) {
                            outputStreams.put(name, new BufferedOutputStream(new FileOutputStream(workingDirectory + '/' + name)));
                        }

                        log.info("CURRENT THREAD: " + Thread.currentThread().getId() + " => " + outputStreams.get(name).toString() + ":" + name);

                        outputStreams.get(name).write(data);
                        outputStreams.get(name).flush();
                    } catch (Exception e) {
                        log.error("EXCEPTION !!", e);
                        e.printStackTrace();
                    }
                }


            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(MessageResponse.newBuilder().setMessageId("dummy1").setMetaMessage("Completed !!").setData("You are now done with streaming").setMetaStatus(MetaStatus.SUCCESS).build());
                responseObserver.onCompleted();
                log.info("Received attachment Fully ");
                if (!outputStreams.isEmpty()) {
                    try {
                        for (BufferedOutputStream outputStream : outputStreams.values()) {
                            try {
                                outputStream.close();
                                log.info("Closing outputStream ");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    } finally {
                        outputStreams.clear();
                    }
                }
            }
        };
    }
}
