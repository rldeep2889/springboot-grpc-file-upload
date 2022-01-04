package com.grpc.file.upload.util;

import io.grpc.stub.StreamObserver;

public abstract class GrpcUtils {

    public static <T> StreamObserver<T> getDefaultStreamObserver() {
        return new StreamObserver<T>() {

            @Override
            public void onNext(T value) {
                System.out.println("on next...");
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("on error...");
            }

            @Override
            public void onCompleted() {
                System.out.println("on completed...");
            }
        };
    }
}