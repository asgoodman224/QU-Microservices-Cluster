import java.io.*;
import java.net.*;
import java.util.*;

/**
 * CSVService.java
 * by Andrew Goodman
 * 
 * This service calculates statistics on CSV data: mean, median, min, max, and standard deviation.
 * 
 * How it works:
 * 1. Sends UDP heartbeats to the server every 15-30 seconds
 * 2. Listens for incoming TCP connections with CSV data
 * 3. Calculates the requested stats and sends them back
 */
public class CSVService {

    // Service configuration
    static String NODE_ID = "CSVNode1";
    static int MY_PORT = 5102;                    // Port this service listens on
    static String SERVER_IP = "127.0.0.1";        // Main server address
    static int SERVER_UDP_PORT = 5001;            // Port for sending heartbeats
    
    static volatile boolean running = true;

    public static void main(String[] args) {
        // Allow command line args to override defaults
        if (args.length >= 1) NODE_ID = args[0];
        if (args.length >= 2) MY_PORT = Integer.parseInt(args[1]);
        if (args.length >= 3) SERVER_IP = args[2];
        if (args.length >= 4) SERVER_UDP_PORT = Integer.parseInt(args[3]);

        System.out.println("=== CSV STATISTICS SERVICE ===");
        System.out.println("Node ID: " + NODE_ID);
        System.out.println("Listening on port: " + MY_PORT);
        System.out.println("Server: " + SERVER_IP + ":" + SERVER_UDP_PORT);
        System.out.println("==============================\n");

        // Start heartbeat thread in the background
        new Thread(CSVService::sendHeartbeats).start();
        
        // Listen for incoming task requests
        listenForTasks();
    }

    // Sends periodic heartbeats to let the server know we're still running
    static void sendHeartbeats() {
        try {
            DatagramSocket socket = new DatagramSocket();
            InetAddress serverAddr = InetAddress.getByName(SERVER_IP);
            Random random = new Random();

            while (running) {
                // Build the heartbeat message
                String message = "HEARTBEAT|" + NODE_ID + "|CSV_STATS|" + MY_PORT;
                byte[] data = message.getBytes();

                // Send it to the server via UDP
                DatagramPacket packet = new DatagramPacket(data, data.length, serverAddr, SERVER_UDP_PORT);
                socket.send(packet);
                System.out.println("[Heartbeat] Sent: " + message);

                // Wait 15-30 seconds before sending the next one
                Thread.sleep(15000 + random.nextInt(15000));
            }
            socket.close();
        } catch (Exception e) {
            System.err.println("Heartbeat error: " + e.getMessage());
        }
    }

    // Listens for incoming TCP connections with tasks
    static void listenForTasks() {
        try {
            ServerSocket serverSocket = new ServerSocket(MY_PORT);
            System.out.println("[TCP] Listening for tasks on port " + MY_PORT + "...\n");

            while (running) {
                // Accept incoming connection
                Socket client = serverSocket.accept();
                System.out.println("[TCP] Received connection from " + client.getInetAddress());

                // Handle in separate thread so we can process multiple requests
                new Thread(() -> handleTask(client)).start();
            }
            serverSocket.close();
        } catch (Exception e) {
            System.err.println("TCP server error: " + e.getMessage());
        }
    }

    // Handles a single CSV statistics request
    static void handleTask(Socket client) {
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());

            // Read the operation request (MEAN, MEDIAN, ALL, etc.)
            String request = in.readUTF();
            String[] parts = request.split("\\|");
            String operation = parts[0];  // MEAN, MEDIAN, ALL, etc.
            System.out.println("[Task] Operation: " + operation);

            // Read the CSV data
            long size = in.readLong();
            byte[] csvData = new byte[(int) size];
            in.readFully(csvData);
            System.out.println("[Task] Received " + size + " bytes of CSV data");

            // Calculate the statistics
            String result = calculateStats(operation, new String(csvData));

            // Send the results back
            byte[] resultBytes = result.getBytes();
            out.writeUTF("OK");
            out.writeLong(resultBytes.length);
            out.write(resultBytes);
            out.flush();
            System.out.println("[Task] Sent statistics back\n");

            client.close();

        } catch (Exception e) {
            System.err.println("Task error: " + e.getMessage());
        }
    }

    // Parses the CSV and calculates the requested statistics
    static String calculateStats(String operation, String csvData) {
        StringBuilder result = new StringBuilder();
        result.append("=== CSV STATISTICS ===\n\n");

        // Split CSV into lines
        String[] lines = csvData.split("\n");
        if (lines.length < 2) {
            return "Error: Need header row and at least one data row";
        }

        // First line contains column headers
        String[] headers = lines[0].split(",");
        int numCols = headers.length;

        // Create a list for each column to store numeric values
        List<List<Double>> columns = new ArrayList<>();
        for (int i = 0; i < numCols; i++) {
            columns.add(new ArrayList<>());
        }

        // Parse each data row
        for (int row = 1; row < lines.length; row++) {
            String[] values = lines[row].split(",");
            for (int col = 0; col < Math.min(values.length, numCols); col++) {
                try {
                    double val = Double.parseDouble(values[col].trim());
                    columns.get(col).add(val);
                } catch (NumberFormatException e) {
                    // Skip non-numeric values
                }
            }
        }

        result.append("Rows: ").append(lines.length - 1).append("\n");
        result.append("Columns: ").append(numCols).append("\n\n");

        // Calculate and output the requested statistics
        for (int col = 0; col < numCols; col++) {
            List<Double> values = columns.get(col);
            if (values.isEmpty()) continue;

            result.append("--- ").append(headers[col].trim()).append(" ---\n");

            if (operation.equals("MEAN") || operation.equals("ALL")) {
                result.append("  Mean: ").append(String.format("%.2f", mean(values))).append("\n");
            }
            if (operation.equals("MEDIAN") || operation.equals("ALL")) {
                result.append("  Median: ").append(String.format("%.2f", median(values))).append("\n");
            }
            if (operation.equals("MIN") || operation.equals("ALL")) {
                result.append("  Min: ").append(String.format("%.2f", Collections.min(values))).append("\n");
            }
            if (operation.equals("MAX") || operation.equals("ALL")) {
                result.append("  Max: ").append(String.format("%.2f", Collections.max(values))).append("\n");
            }
            if (operation.equals("STDDEV") || operation.equals("ALL")) {
                result.append("  StdDev: ").append(String.format("%.2f", stddev(values))).append("\n");
            }
            result.append("\n");
        }

        return result.toString();
    }

    // Statistics Functions 

    // Calculates the mean (average) of a list of values
    static double mean(List<Double> values) {
        double sum = 0;
        for (double v : values) sum += v;
        return sum / values.size();
    }

    // Calculates the median (middle value) of a sorted list
    static double median(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        if (n % 2 == 0) {
            return (sorted.get(n/2 - 1) + sorted.get(n/2)) / 2.0;
        } else {
            return sorted.get(n/2);
        }
    }

    // Calculates the standard deviation (measure of spread)
    static double stddev(List<Double> values) {
        double avg = mean(values);
        double sumSquares = 0;
        for (double v : values) {
            sumSquares += (v - avg) * (v - avg);
        }
        return Math.sqrt(sumSquares / values.size());
    }
}
