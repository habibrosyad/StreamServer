package edu.monash.streaming.streamserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Receiver {
    public static final Tuple POISON = new Tuple(0,"", "","");

    private final int port;
    private final Map<String, BlockingQueue<Tuple>> buffer;
    private final BlockingQueue<Tuple> flattenedBuffer;
    private final Service service;
    private final String[] streams;

    public Receiver(int port, String[] streams) {
        this.port = port;
        this.streams = streams;

        buffer = new HashMap<>();
        for (String stream: streams) {
            buffer.put(stream, new LinkedBlockingQueue<>());
        }

        flattenedBuffer = new LinkedBlockingQueue<>();
        service = new Service(streams.length);
    }

    public BlockingQueue<Tuple> getFlattenedBuffer() { return flattenedBuffer; }

    public Map<String, BlockingQueue<Tuple>> getBuffer() {
        return buffer;
    }

    public String[] getStreams() {
        return streams;
    }

    public void start() {
        // Define receiver task
        Thread receiver = new Thread(() -> {
            try {
                // Define connection
                ServerSocket serverSocket = new ServerSocket(port);
                // Waiting connection
                System.out.println("Waiting for clients to connect...");
                while (!service.getStopped().get()) {
                    Socket clientSocket = serverSocket.accept();
                    service.getPool().submit(new Reader(clientSocket));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        receiver.start();
    }

    public void stop() throws InterruptedException {
        // Prevent multiple execution on this method
        if (service.getStopped().compareAndSet(false,true)) {
            // Shutdown pool
            System.out.println("Shutting down receicer...");
            service.getPool().shutdown();

            // Immediately terminate, because we already know the completion status
            service.getPool().awaitTermination(1, TimeUnit.MILLISECONDS);

            // Insert poison to buffer for shutting down the joiner
            for (String stream: buffer.keySet()) {
                buffer.get(stream).put(POISON);
            }

            // Also add poison to the flattened buffer
            flattenedBuffer.put(POISON);
        }
    }

    private class Reader implements Runnable {
        private final Socket clientSocket;

        public Reader(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run(){
            System.out.println("Got a client on " + Thread.currentThread().getName());
            try {
                BufferedReader inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String message;

                while ((message = inputStream.readLine()) != null && !service.getStopped().get()) {
                    // Stream ended with the empty string
                    if (message.equals("")) {
                        service.getCompletion().increment();
                        // Shutdown if complete\
                        if (service.getCompletion().sum() == streams.length) {
                            stop();
                        }
                        break;
                    }

                    String[] data = message.split(":");
                    Tuple tuple = new Tuple(System.currentTimeMillis(), data[0], data[1], data[2]);

                    // If stream name doesn't exists don't do anything
                    if (buffer.containsKey(data[0])) {
                        // Store data to the buffer
                        buffer.get(data[0]).put(tuple);

                        // Add also to the flattened buffer
                        flattenedBuffer.put(tuple);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
