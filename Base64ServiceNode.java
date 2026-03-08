package cluster;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

/**
 * Service Node: Base64 Encode / Decode
 */
public class Base64ServiceNode {

    // ===== CONFIG =====
    private static final String NODE_ID = "SN1";
    private static final String SERVICE = "BASE64";

    private static final String SERVER_IP = "172.31.44.92";
    private static final int SERVER_UDP_PORT = 5001;

    private static final int SN_TCP_PORT = 6001;

    // ==================

    public static void main(String[] args) throws Exception {

        System.out.println("Starting Base64 Service Node...");

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

        try (
            BufferedReader in =
                new BufferedReader(new InputStreamReader(socket.getInputStream()));

            BufferedWriter out =
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
        ) {

            /*
             * Protocol:
             * ENCODE|text
             * DECODE|base64text
             */

            String line = in.readLine();

            if (line == null) return;

            String[] parts = line.split("\\|", 2);

            String command = parts[0];
            String data = parts.length > 1 ? parts[1] : "";

            String result;

            if ("ENCODE".equals(command)) {

                result = Base64.getEncoder()
                        .encodeToString(data.getBytes(StandardCharsets.UTF_8));

            } else if ("DECODE".equals(command)) {

                byte[] decoded =
                        Base64.getDecoder().decode(data);

                result = new String(decoded, StandardCharsets.UTF_8);

            } else {

                result = "ERROR|Unknown command";
            }

            out.write(result);
            out.newLine();
            out.flush();

        } catch (Exception e) {

            System.err.println("Client error: " + e.getMessage());

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

                    String msg =
                            "HEARTBEAT|" +
                            NODE_ID + "|" +
                            SERVICE + "|" +
                            SN_TCP_PORT;

                    byte[] data = msg.getBytes();

                    DatagramPacket packet =
                        new DatagramPacket(
                            data,
                            data.length,
                            InetAddress.getByName(SERVER_IP),
                            SERVER_UDP_PORT
                        );

                    socket.send(packet);

                    System.out.println("Sent heartbeat");

                    int wait =
                        15000 + rand.nextInt(15000);

                    Thread.sleep(wait);
                }

            } catch (Exception e) {

                System.err.println("Heartbeat error: " + e.getMessage());
            }
        }
    }
}