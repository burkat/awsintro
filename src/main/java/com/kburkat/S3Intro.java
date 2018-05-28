package com.kburkat;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3Intro {

    private static final AmazonS3 S3 = AmazonS3ClientBuilder.defaultClient();
    private static final String KEY = "download.jpg";
    private static final String BUCKET = "burkat";
    private static final String UPLOAD_FILE = "upload.jpg";

    public static void main(String[] args) {

        long start1 = System.currentTimeMillis();
        S3Object s3Object = getObject(BUCKET, KEY);
        long stop1 = System.currentTimeMillis();
        long delta1 = stop1 - start1;
        System.out.println("Download took " + delta1 / 1000.0 + " seconds.");

        saveToFile(s3Object, KEY);
        long stop2 = System.currentTimeMillis();
        long delta2 = stop2 - stop1;
        System.out.println("Download with saving took " + delta2 / 1000.0 + " seconds.");

        long start3 = System.currentTimeMillis();
        putObject(BUCKET, UPLOAD_FILE, UPLOAD_FILE);
        long stop3 = System.currentTimeMillis();
        long delta3 = stop3 - start3;
        System.out.println("Upload took " + delta3 / 1000.0 + " seconds.");

        ExecutorService executorService = Executors.newCachedThreadPool();
        final File file = new File(UPLOAD_FILE);
        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> S3.putObject(BUCKET, UPLOAD_FILE, file));
        }
        long start4 = System.currentTimeMillis();
        putObject(BUCKET, UPLOAD_FILE, UPLOAD_FILE);
        long stop4 = System.currentTimeMillis();
        long delta4 = stop4 - start4;
        System.out.println("High load upload took " + delta4 / 1000.0 + " seconds.");
        executorService.shutdown();
    }

    private static S3Object getObject(String bucketName, String key) {

        S3Object s3Object = null;
        try {
            s3Object = S3.getObject(bucketName, key);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        return s3Object;
    }

    private static void putObject(String bucketName, String key, String filePath) {
        try {
            File file = new File(filePath);
            S3.putObject(bucketName, key, file);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private static void saveToFile(S3Object s3Object, String fileName) {
        try {
            Objects.requireNonNull(s3Object);
            S3ObjectInputStream s3is = s3Object.getObjectContent();
            FileOutputStream fos = new FileOutputStream(new File(fileName));
            byte[] read_buf = new byte[1024];
            int read_len;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
