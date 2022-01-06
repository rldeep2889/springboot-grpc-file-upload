package com.grpc.file.upload.streaming;

import com.grpc.file.upload.proto.ncclient.Attachment;
import com.grpc.file.upload.proto.ncclient.FileUploadServiceGrpc;
import com.grpc.file.upload.proto.ncclient.PublishMessageRequest;
import com.grpc.file.upload.util.FileUtils;
import com.grpc.file.upload.util.GrpcUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Client to publish meta data and attachments together
 */
@Slf4j
public class FileServiceClient2 {

    public static void main(final String[] args) throws InterruptedException {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("provide (at least 1) filepath as argument");
        }

        final ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("localhost", 9090).usePlaintext().build();
        final FileUploadServiceGrpc.FileUploadServiceStub stub = FileUploadServiceGrpc.newStub(managedChannel);

        Map<String, Object> metaData = new HashMap<>();
        metaData.put("source_id", 2);
        metaData.put("message", "sample with id = 2");

        uploadFile(
                stub,
                metaData,
                new ArrayList<>(Arrays.asList(args))
        );

        managedChannel.awaitTermination(50, TimeUnit.SECONDS);
    }

    public static void uploadFile(final FileUploadServiceGrpc.FileUploadServiceStub stub, final Map<String, Object> metaData, final List<String> filepaths) {
        StreamObserver<PublishMessageRequest> requestObserver = stub.publishMessage(GrpcUtils.getDefaultStreamObserver());

        try {
//            PublishMessageRequest formPart = PublishMessageRequest.newBuilder()
//                    .setSourceId(Integer.parseInt(metaData.getOrDefault("source_id", 0).toString()))
//                    .setMessage(metaData.getOrDefault("message", "_no_message").toString())
//                    .build();
//
//            requestObserver.onNext(formPart);

            while (!filepaths.isEmpty()) {
                final String filepath = filepaths.remove(0);

                FileUtils.streamFile(filepath, requestObserver, (filename, byteString) -> PublishMessageRequest.newBuilder()
                        .setSourceId(Integer.parseInt(metaData.getOrDefault("source_id", 0).toString()))
                        .setMessage(metaData.getOrDefault("message", "_no_message").toString())
                        .addAttachments(
                                Attachment.newBuilder().
                                        setAttachmentBytes(byteString)
                                        .setAttachmentName(filename)
                                        .build()
                        )
                        .addAttachments(
                                Attachment.newBuilder().
                                        setAttachmentBytes(byteString)
                                        .setAttachmentName("copy_" + filename)
                                        .build())
                        .addAttachments(
                                Attachment.newBuilder().
                                        setAttachmentBytes(byteString)
                                        .setAttachmentName("3rd_copy_" + filename)
                                        .build()
                        )
                        .build()
                );

            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            log.error("EXCEPTION !!", e);
            throw e;
        }
        requestObserver.onCompleted();
    }
}