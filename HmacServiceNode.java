import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import javax.crypto.Mac;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
        System.out.println("HMAC verification running on port " + TCP_PORT);

        while (true) {
            Socket client = serverSocket.accept();
            handleClient(client);
        }
    }

    private static void handleClient(Socket client) {
        try {

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream()));

            PrintWriter out = new PrintWriter(
                    client.getOutputStream(), true);

            String body = in.readLine();

            String message = extractValue(body, "message");
            String signature = extractValue(body, "signature");

            String expectedSignature = createHmac(message);

            if (expectedSignature.equals(signature)) {
                System.out.println("Message verified: " + message);
                out.println("Valid message");
            } else {
                System.out.println("Invalid signature");
                out.println("Invalid signature");
            }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String createHmac(String message) throws Exception {

        Mac mac = Mac.getInstance("HmacSHA256");

        SecretKeySpec keySpec = new SecretKeySpec(SECRET.getBytes(), "HmacSHA256");

        mac.init(keySpec);

        byte[] rawHmac = mac.doFinal(message.getBytes());

        return bytesToHex(rawHmac);
    }

    private static String bytesToHex(byte[] bytes) {

        StringBuilder hex = new StringBuilder();

        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }

        return hex.toString();
    }

    private static String extractValue(String json, String key) {

        if (json == null)
            return "";

        String pattern = "\"" + key + "\":\"";
        int start = json.indexOf(pattern);

        if (start == -1)
            return "";

        start += pattern.length();

        int end = json.indexOf("\"", start);

        return json.substring(start, end);
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