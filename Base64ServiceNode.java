import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import java.util.Random;

/**
 * Service Node: Base64 Encode / Decode
 *
 * Binary protocol (same as CSV and Image services):
 *   Request:  readUTF("ENCODE|") or readUTF("DECODE|")
 *             readLong(dataSize)
 *             readFully(data)
 *   Response: writeUTF("OK" or "ERROR")
 *             writeLong(resultSize)
 *             write(result)
 *
 * ENCODE: takes raw bytes (e.g. a file), returns base64 text as bytes.
 * DECODE: takes base64 text as bytes, returns decoded raw bytes.
 */
public class Base64ServiceNode {

    // ===== CONFIG =====
    private static final String NODE_ID = "SN1";
    private static final String SERVICE = "BASE64";

    private static String SERVER_IP;
    private static int SERVER_UDP_PORT;
    private static int SN_TCP_PORT;

    // ==================

    public static void main(String[] args) throws Exception {

        System.out.println("Starting Base64 Service Node...");

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
        SERVER_IP = config.getProperty("server.ip");
        SERVER_UDP_PORT = Integer.parseInt(config.getProperty("server.udp.port"));
        SN_TCP_PORT = Integer.parseInt(config.getProperty("sn.tcp.port"));

        // Start heartbeat
        new Thread(new HeartbeatSender()).start();

        // Start TCP server
        startTcpServer();
    }

    /* ================= TCP SERVER ================= */

    private static void startTcpServer() throws Exception {

        ServerSocket serverSocket = new ServerSocket(SN_TCP_PORT);

        System.out.println("Listening on TCP port " + SN_TCP_PORT);

        while (true) {

            Socket socket = serverSocket.accept();

            new Thread(() -> handleClient(socket)).start();
        }
    }

    private static void handleClient(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // Read operation
            String request = in.readUTF();
            String[] parts = request.split("\\|");
            String command = parts[0].toUpperCase();
            System.out.println("[Task] Operation: " + command);

            // Read data payload (file bytes)
            long size = in.readLong();
            byte[] data = new byte[(int) size];
            in.readFully(data);
            System.out.println("[Task] Received " + size + " bytes");

            byte[] result;

            if ("ENCODE".equals(command)) {
                // Raw bytes in -> base64 text out
                String encoded = Base64.getEncoder().encodeToString(data);
                result = encoded.getBytes(StandardCharsets.UTF_8);
                System.out.println("[Task] Encoded " + size + " bytes -> " + result.length + " chars of base64");

            } else if ("DECODE".equals(command)) {
                // Base64 text in -> raw bytes out
                String base64Text = new String(data, StandardCharsets.UTF_8).trim();
                result = Base64.getDecoder().decode(base64Text);
                System.out.println("[Task] Decoded " + size + " chars of base64 -> " + result.length + " bytes");

            } else {
                String error = "Unknown command: " + command;
                out.writeUTF("ERROR");
                out.writeLong(error.length());
                out.write(error.getBytes());
                out.flush();
                socket.close();
                return;
            }

            out.writeUTF("OK");
            out.writeLong(result.length);
            out.write(result);
            out.flush();
            System.out.println("[Task] Sent result back\n");

            socket.close();

        } catch (Exception e) {
            System.err.println("Task error: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (Exception ignored) {}
        }
    }

    /* ================= HEARTBEAT ================= */

    static class HeartbeatSender implements Runnable {

        @Override
        public void run() {

            Random rand = new Random();

            try (DatagramSocket socket = new DatagramSocket()) {

                while (true) {

                    String msg = "HEARTBEAT|" +
                            NODE_ID + "|" +
                            SERVICE + "|" +
                            SN_TCP_PORT;

                    byte[] data = msg.getBytes();

                    DatagramPacket packet = new DatagramPacket(
                            data,
                            data.length,
                            InetAddress.getByName(SERVER_IP),
                            SERVER_UDP_PORT);

                    socket.send(packet);

                    System.out.println("Sent heartbeat");

                    int wait = 15000 + rand.nextInt(15000);

                    Thread.sleep(wait);
                }

            } catch (Exception e) {

                System.err.println("Heartbeat error: " + e.getMessage());
            }
        }
    }
}