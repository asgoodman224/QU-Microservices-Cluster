import java.io.*;
import java.net.*;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionServiceNode {

    private static final int TCP_PORT = 3000;

    private static final String SERVER_HOST = "172.31.44.92";
    private static final int SERVER_PORT = 5001;

    public static void main(String[] args) throws Exception {

        // Start heartbeat thread
        startHeartbeat();

        ServerSocket serverSocket = new ServerSocket(TCP_PORT);
        System.out.println("Compression Service running on port " + TCP_PORT);

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

            String action = extractValue(body, "action");
            String data = extractValue(body, "data");

            if (action.equals("compress")) {

                String result = compress(data);
                out.println(result);
                System.out.println("Compressed data sent");

            } else if (action.equals("decompress")) {

                String result = decompress(data);
                out.println(result);
                System.out.println("Decompressed data sent");

            } else {

                out.println("Invalid action");
            }

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // GZIP COMPRESS
    private static String compress(String str) throws Exception {

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        GZIPOutputStream gzip = new GZIPOutputStream(byteStream);

        gzip.write(str.getBytes());

        gzip.close();

        return Base64.getEncoder().encodeToString(byteStream.toByteArray());
    }

    // GZIP DECOMPRESS
    private static String decompress(String compressed) throws Exception {

        byte[] decoded = Base64.getDecoder().decode(compressed);

        ByteArrayInputStream byteStream = new ByteArrayInputStream(decoded);

        GZIPInputStream gzip = new GZIPInputStream(byteStream);

        BufferedReader reader = new BufferedReader(new InputStreamReader(gzip));

        StringBuilder output = new StringBuilder();

        String line;

        while ((line = reader.readLine()) != null) {
            output.append(line);
        }

        return output.toString();
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

                String message = "{\"type\":\"heartbeat\",\"service\":\"compression\",\"port\":3000}";

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