package com.kburkat;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.WaiterParameters;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class EC2Intro {

    private static final String INSTANCE_ID = "i-0d6c4b22acea4072d";
    private static final AmazonEC2 EC2 = AmazonEC2ClientBuilder.defaultClient();

    public static void main(String[] args) throws Exception {
        long tStart = System.currentTimeMillis();
        startInstance(INSTANCE_ID);
        waitForInstanceSetup(INSTANCE_ID);
        waitForHttpServerSetup(getInstancePublicIpv4(INSTANCE_ID));
        saveFileFromHttpServer(getInstancePublicIpv4(INSTANCE_ID));
        stopInstance(INSTANCE_ID);
        waitForInstanceStop(INSTANCE_ID);
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.out.println("It took " + elapsedSeconds + " seconds.");
    }

    private static void startInstance(String instanceID) {
        StartInstancesRequest startInstancesRequest = new StartInstancesRequest()
                .withInstanceIds(instanceID);

        EC2.startInstances(startInstancesRequest);
    }

    private static void stopInstance(String instanceID) {
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest()
                .withInstanceIds(instanceID);
        EC2.stopInstances(stopInstancesRequest);
    }

    private static void waitForInstanceSetup(String instanceID) {
        DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(instanceID);
        PollingStrategy strategy = new PollingStrategy(new MaxAttemptsRetryStrategy(45), new FixedDelayStrategy(2));
        WaiterParameters<DescribeInstancesRequest> parameters = new WaiterParameters<>(request).withPollingStrategy(strategy);
        EC2.waiters().instanceRunning().run(parameters);
    }

    private static void waitForInstanceStop(String instanceID) {
        DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(instanceID);
        PollingStrategy strategy = new PollingStrategy(new MaxAttemptsRetryStrategy(45), new FixedDelayStrategy(2));
        WaiterParameters<DescribeInstancesRequest> parameters = new WaiterParameters<>(request).withPollingStrategy(strategy);
        EC2.waiters().instanceStopped().run(parameters);
    }

    private static String getInstancePublicIpv4(String instanceID) {
        return EC2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceID))
                .getReservations().get(0).getInstances().get(0).getPublicIpAddress();
    }

    private static void saveFileFromHttpServer(String address) throws IOException {
        URL url = new URL("http", address, "/index.html");
        ReadableByteChannel rbc = Channels.newChannel(url.openStream());
        FileOutputStream fos = new FileOutputStream("index.html");
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        rbc.close();
        fos.close();
    }

    private static void waitForHttpServerSetup(String address) throws MalformedURLException {
        URL url = new URL("http", address, "/index.html");

        boolean connected = false;
        while (!connected) {
            try {
                url.openStream();
                connected = true;
            } catch (IOException ignored) {
            }
        }
    }

}
