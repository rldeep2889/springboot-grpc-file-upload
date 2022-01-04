package com.grpc.file.upload.util;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.function.BiFunction;

public abstract class FileUtils {

    public static void streamFile(String filepath, StreamObserver requestObserver, BiFunction<String, ByteString, ?> requestProvider) {
        try {
            final File file = Paths.get(filepath).toFile();
            BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(file));
            int bufferSize = 1024 * 1024;
            byte[] buffer = new byte[bufferSize];
            int size;
            while ((size = bInputStream.read(buffer)) > 0) {
                ByteString byteString = ByteString.copyFrom(buffer, 0, size);
                requestObserver.onNext(requestProvider.apply(FileUtils.getSimpleFilename(filepath), byteString));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static String getSimpleFilename(String filepath) {
        if (filepath.indexOf('/') != -1) {
            return filepath.substring(filepath.lastIndexOf("/") + 1);
        }
        return filepath;
    }
}
