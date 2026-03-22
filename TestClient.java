import java.io.*;
import java.net.*;
import java.nio.file.*;

/**
 * TestClient.java
 * by Andrew Goodman
 * 
 * A simple client for testing the service nodes directly without the main server.
 * 
 * Usage:
 *   java TestClient image grayscale photo.png
 *   java TestClient csv all data.csv
 */
public class TestClient {

    static String HOST = "127.0.0.1";
    static int IMAGE_PORT = 5101;
    static int CSV_PORT = 5102;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage:");
            System.out.println("  java TestClient image <grayscale|resize|rotate|thumbnail> <file> [params]");
            System.out.println("  java TestClient csv <all|mean|median|min|max|stddev> <file>");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  java TestClient image grayscale photo.png");
            System.out.println("  java TestClient image resize photo.png 400x300");
            System.out.println("  java TestClient csv all data.csv");
            return;
        }

        String service = args[0];
        String operation = args[1].toUpperCase();
        String filename = args[2];
        String params = args.length > 3 ? args[3] : "";

        // Read the input file
        byte[] inputData = Files.readAllBytes(Paths.get(filename));
        System.out.println("Read " + inputData.length + " bytes from " + filename);

        // Connect to the appropriate service based on type
        int port = service.equals("image") ? IMAGE_PORT : CSV_PORT;
        System.out.println("Connecting to " + HOST + ":" + port + "...");

        Socket socket = new Socket(HOST, port);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());

        // Send the request
        out.writeUTF(operation + "|" + params);
        out.writeLong(inputData.length);
        out.write(inputData);
        out.flush();
        System.out.println("Sent request: " + operation);

        // Receive the response
        String status = in.readUTF();
        long resultSize = in.readLong();
        System.out.println("Response: " + status + ", " + resultSize + " bytes");

        if (resultSize > 0) {
            byte[] result = new byte[(int) resultSize];
            in.readFully(result);

            if (service.equals("image")) {
                // Save the result image with a modified filename
                int lastDot = filename.lastIndexOf(".");
                String outFile = filename.substring(0, lastDot) + "_" + operation.toLowerCase() + filename.substring(lastDot);
                Files.write(Paths.get(outFile), result);
                System.out.println("Saved result to: " + outFile);
            } else {
                // Print CSV statistics to console
                System.out.println("\n" + new String(result));
            }
        }

        socket.close();
    }
}
