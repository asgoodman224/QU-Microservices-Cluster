import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HmacServiceNode {

    private static final String SECRET = "this_is_a_secret_key";
    private static final int TCP_PORT = 3000;

    private static final String SERVER_HOST = "172.31.44.92";
    private static final int SERVER_PORT = 5001;

    public static void main(String[] args) throws Exception {

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

                String message = "{\"type\":\"heartbeat\",\"service\":\"hmac_verify\",\"port\":3000}";

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