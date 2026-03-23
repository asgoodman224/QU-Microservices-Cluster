import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * CompressionServiceNode — GZIP compress/decompress service.
 *
 * Binary protocol (same as CSV and Image services):
 *   Request:  readUTF("COMPRESS|") or readUTF("DECOMPRESS|")
 *             readLong(dataSize)
 *             readFully(data)
 *   Response: writeUTF("OK" or "ERROR")
 *             writeLong(resultSize)
 *             write(result)
 *
 * Works with raw file bytes — not just text strings.
 */
public class CompressionServiceNode {

    private static int TCP_PORT;
    private static String SERVER_HOST;
    private static int SERVER_PORT;

    public static void main(String[] args) throws Exception {

        // Load config.properties
        Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            config.load(fis);
        } catch (IOException e) {
            System.out.println("Failed to load config.properties");
            e.printStackTrace();
            return; // exit if config not found
        }

        // Initialize variables from config
        TCP_PORT = Integer.parseInt(config.getProperty("tcp.port"));
        SERVER_HOST = config.getProperty("server.host");
        SERVER_PORT = Integer.parseInt(config.getProperty("server.port"));

        // Start heartbeat thread
        startHeartbeat();

        ServerSocket serverSocket = new ServerSocket(TCP_PORT);
        System.out.println("Compression Service running on port " + TCP_PORT);

        while (true) {
            Socket client = serverSocket.accept();
            new Thread(() -> handleClient(client)).start();
        }
    }

    private static void handleClient(Socket client) {
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());

            // Read operation
            String request = in.readUTF();
            String[] parts = request.split("\\|");
            String operation = parts[0].toUpperCase();
            System.out.println("[Task] Operation: " + operation);

            // Read data payload (file bytes)
            long size = in.readLong();
            byte[] data = new byte[(int) size];
            in.readFully(data);
            System.out.println("[Task] Received " + size + " bytes");

            byte[] result;

            if ("COMPRESS".equals(operation)) {
                result = compress(data);
                System.out.println("[Task] Compressed " + size + " -> " + result.length + " bytes");
            } else if ("DECOMPRESS".equals(operation)) {
                result = decompress(data);
                System.out.println("[Task] Decompressed " + size + " -> " + result.length + " bytes");
            } else {
                String error = "Unknown operation: " + operation;
                out.writeUTF("ERROR");
                out.writeLong(error.length());
                out.write(error.getBytes());
                out.flush();
                client.close();
                return;
            }

            out.writeUTF("OK");
            out.writeLong(result.length);
            out.write(result);
            out.flush();
            System.out.println("[Task] Sent result back\n");

            client.close();

        } catch (Exception e) {
            System.err.println("Task error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // GZIP COMPRESS — works on raw bytes (any file type)
    private static byte[] compress(byte[] data) throws Exception {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(byteStream);
        gzip.write(data);
        gzip.close();
        return byteStream.toByteArray();
    }

    // GZIP DECOMPRESS — works on raw bytes
    private static byte[] decompress(byte[] compressed) throws Exception {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(compressed);
        GZIPInputStream gzip = new GZIPInputStream(byteStream);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int len;
        while ((len = gzip.read(buffer)) != -1) {
            output.write(buffer, 0, len);
        }
        gzip.close();
        return output.toByteArray();
    }

    // UDP HEARTBEAT

    private static void startHeartbeat() {

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(() -> {

            try {

                String message = "{\"type\":\"heartbeat\",\"service\":\"compression\",\"port\":" + TCP_PORT + "}";

                byte[] buffer = message.getBytes();

                DatagramSocket socket = new DatagramSocket();

                DatagramPacket packet = new DatagramPacket(
                        buffer,
                        buffer.length,
                        InetAddress.getByName(SERVER_HOST),
                        SERVER_PORT);

                socket.send(packet);

                socket.close();

                System.out.println("Heartbeat sent");

            } catch (Exception e) {
                System.out.println("Heartbeat error: " + e.getMessage());
            }

        }, 0, 30, TimeUnit.SECONDS);
    }
}