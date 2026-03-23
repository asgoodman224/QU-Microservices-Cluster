import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
 
/**
 * HmacServiceNode — HMAC-SHA256 signing and verification service.
 *
 * Binary protocol (same as CSV and Image services):
 *   Request:  readUTF("SIGN|") or readUTF("VERIFY|<signature>")
 *             readLong(dataSize)
 *             readFully(data)
 *   Response: writeUTF("OK" or "ERROR")
 *             writeLong(resultSize)
 *             write(result)
 *
 * SIGN:   takes raw file bytes, returns the hex HMAC-SHA256 signature.
 * VERIFY: takes raw file bytes + signature in the request header,
 *         returns "Valid" or "Invalid".
 */
public class HmacServiceNode {
 
    private static String SECRET;
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
        SECRET = config.getProperty("secret");
        TCP_PORT = Integer.parseInt(config.getProperty("tcp.port"));
        SERVER_HOST = config.getProperty("server.host");
        SERVER_PORT = Integer.parseInt(config.getProperty("server.port"));
 
        // Start heartbeat thread
        startHeartbeat();
 
        ServerSocket serverSocket = new ServerSocket(TCP_PORT);
        System.out.println("HMAC Service running on port " + TCP_PORT);
 
        while (true) {
            Socket client = serverSocket.accept();
            new Thread(() -> handleClient(client)).start();
        }
    }
 
    private static void handleClient(Socket client) {
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());
 
            // Read operation — e.g. "SIGN|" or "VERIFY|abc123..."
            String request = in.readUTF();
            String[] parts = request.split("\\|", 2);
            String operation = parts[0].toUpperCase();
            String params = parts.length > 1 ? parts[1] : "";
            System.out.println("[Task] Operation: " + operation);
 
            // Read data payload (file bytes or message bytes)
            long size = in.readLong();
            byte[] data = new byte[(int) size];
            in.readFully(data);
            System.out.println("[Task] Received " + size + " bytes");
 
            byte[] result;
 
            if ("SIGN".equals(operation)) {
                // Compute HMAC of the raw data and return the hex signature
                String signature = createHmac(data);
                result = signature.getBytes(StandardCharsets.UTF_8);
                System.out.println("[Task] Signed — signature: " + signature);
 
            } else if ("VERIFY".equals(operation)) {
                // Compute HMAC and compare to provided signature
                String expectedSignature = createHmac(data);
                String providedSignature = params.trim();
 
                if (expectedSignature.equals(providedSignature)) {
                    result = "Valid".getBytes(StandardCharsets.UTF_8);
                    System.out.println("[Task] Signature is VALID");
                } else {
                    result = "Invalid".getBytes(StandardCharsets.UTF_8);
                    System.out.println("[Task] Signature is INVALID");
                    System.out.println("       Expected: " + expectedSignature);
                    System.out.println("       Got:      " + providedSignature);
                }
 
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
 
    // Computes HMAC-SHA256 over raw bytes (works on any file)
    private static String createHmac(byte[] data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec keySpec = new SecretKeySpec(SECRET.getBytes(), "HmacSHA256");
        mac.init(keySpec);
 
        byte[] rawHmac = mac.doFinal(data);
        return bytesToHex(rawHmac);
    }
 
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
 
    // UDP HEARTBEAT
 
    private static void startHeartbeat() {
 
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
 
        scheduler.scheduleAtFixedRate(() -> {
 
            try {
 
                String message = "{\"type\":\"heartbeat\",\"service\":\"hmac_verify\",\"port\":" + TCP_PORT + "}";
 
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
 