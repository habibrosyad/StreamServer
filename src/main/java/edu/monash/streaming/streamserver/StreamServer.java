package edu.monash.streaming.streamserver;

import edu.monash.streaming.streamserver.algorithm.JoinType;
import org.apache.commons.cli.*;

public class StreamServer {
    /**
     * java -jar StreamServer.jar -p 9999 -a mjoin -s rstream.txt:sstream.txt:tstream.txt:uStream.txt -W 10 -T 1 -Te 1 -t 60
     * @param args
     */
    public static void main(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addRequiredOption("p", "port", true, "port number");
        options.addRequiredOption("s", "streams", true, "sequence of stream in lower case separated by ':'");
        options.addOption("j", true, "join algorithm: mjoin, amjoin, tswjoin");
        options.addOption("W", true, "window size (seconds)");
        options.addOption("T", true, "slide time (seconds)");
        options.addOption("Te", true, "join evaluation time (seconds)");
        options.addOption("t", true, "set execution time for the server (seconds)");

        try {
            // parse the command line arguments
            CommandLine cmd = parser.parse(options, args);

            // Default parameters
            int port = Integer.parseInt(cmd.getOptionValue("p", "4444"));
            String joinType = cmd.getOptionValue("j", "mjoin").toUpperCase();
            // This can also be used as the probing sequence
            String[] streams = cmd.getOptionValue("s").toLowerCase().split(":");
            // Window size
            long W = Integer.parseInt(cmd.getOptionValue("W", "4"))*1000;
            // Sliding time
            long T = Integer.parseInt(cmd.getOptionValue("T", "2"))*1000;
            // Only for time-slide window join
            long Te = Integer.parseInt(cmd.getOptionValue("Te", "1"))*1000;
            // Server execution time, 0 -> no limit
            long t = Integer.parseInt(cmd.getOptionValue("t", "120"))*1000;

            // Setup receiver
            Receiver receiver = new Receiver(port, streams);
            // Setup continuous query (joiner)
            Joiner joiner = new Joiner(receiver, JoinType.valueOf(joinType), W, T, Te);

            // Start process
            joiner.start();
            receiver.start();

            // If execution time is defined
            if (t != 0) {
                // Wait for t seconds
                Thread.sleep(t);

                // Stop receiver will trigger joiner to be stopped
                // This has no effect if receiver has been stopped early
                receiver.stop();
            }

        } catch (Exception e) {
            System.out.println("Unexpected error:" +e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "StreamServer", options);
        }
    }
}
