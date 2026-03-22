package cluster;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Unified test client for QU Cluster Server.
 *
 * Talks to ServerMain on TCP port 5100 using the server's binary protocol.
 *
 * Supported services:
 * - CSV_STATS
 * - IMAGE_TRANSFORM
 * - BASE64
 * - compression
 * - hmac_verify
 *
 * Examples:
 * java cluster.TestClient list 172.31.44.92 5100
 *
 * java cluster.TestClient csv 172.31.44.92 5100 all data.csv
 * java cluster.TestClient csv 172.31.44.92 5100 mean data.csv
 *
 * java cluster.TestClient image 172.31.44.92 5100 grayscale photo.png
 * java cluster.TestClient image 172.31.44.92 5100 resize photo.png 400x300
 * java cluster.TestClient image 172.31.44.92 5100 rotate photo.png 90
 * java cluster.TestClient image 172.31.44.92 5100 thumbnail photo.png 200
 *
 * java cluster.TestClient base64 172.31.44.92 5100 encode "hello world"
 * java cluster.TestClient base64 172.31.44.92 5100 decode "aGVsbG8gd29ybGQ="
 *
 * java cluster.TestClient compression 172.31.44.92 5100 compress "hello world"
 * java cluster.TestClient compression 172.31.44.92 5100 decompress
 * "H4sIAAAAAAAA..."
 *
 * java cluster.TestClient hmac 172.31.44.92 5100 verify "hello world"
 * java cluster.TestClient hmac 172.31.44.92 5100 verify "hello world"
 * <signature>
 */
public class TestClient2 {

    private static final String HMAC_SECRET = "this_is_a_secret_key";

    public static void main(String[] args) {
        if (args.length < 3) {
            printUsage();
            return;
        }

        String mode = args[0].toLowerCase();
        String serverHost = args[1];
        int serverPort;

        try {
            serverPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid server port: " + args[2]);
            return;
        }

        try {
            switch (mode) {
                case "list":
                    runList(serverHost, serverPort);
                    break;
                case "csv":
                    runCsv(serverHost, serverPort, args);
                    break;
                case "image":
                    runImage(serverHost, serverPort, args);
                    break;
                case "base64":
                    runBase64(serverHost, serverPort, args);
                    break;
                case "compression":
                    runCompression(serverHost, serverPort, args);
                    break;
                case "hmac":
                    runHmac(serverHost, serverPort, args);
                    break;
                default:
                    System.err.println("Unknown mode: " + mode);
                    printUsage();
            }
        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // =========================
    // LIST
    // =========================

    private static void runList(String host, int port) throws Exception {
        ServerResponse resp = sendBinaryRequest(host, port, "LIST", new byte[0]);

        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // CSV
    // =========================

    private static void runCsv(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: java cluster.TestClient csv <serverHost> <serverPort> <all|mean|median|min|max|stddev> <file.csv>");
            return;
        }

        String operation = args[3].toUpperCase();
        String filename = args[4];
        byte[] csvBytes = Files.readAllBytes(Path.of(filename));

        String request = "RUN|CSV_STATS|" + operation + "|";

        System.out.println("Sending CSV request through server...");
        System.out.println("Operation: " + operation);
        System.out.println("File: " + filename + " (" + csvBytes.length + " bytes)");

        ServerResponse resp = sendBinaryRequest(host, port, request, csvBytes);

        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // IMAGE
    // =========================

    private static void runImage(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: java cluster.TestClient image <serverHost> <serverPort> <grayscale|resize|rotate|thumbnail> <imageFile> [params]");
            return;
        }

        String operation = args[3].toUpperCase();
        String filename = args[4];
        String params = args.length >= 6 ? args[5] : "";
        byte[] imageBytes = Files.readAllBytes(Path.of(filename));

        String request = "RUN|IMAGE_TRANSFORM|" + operation + "|" + params;

        System.out.println("Sending image request through server...");
        System.out.println("Operation: " + operation + (params.isBlank() ? "" : " (" + params + ")"));
        System.out.println("File: " + filename + " (" + imageBytes.length + " bytes)");

        ServerResponse resp = sendBinaryRequest(host, port, request, imageBytes);

        System.out.println("Status: " + resp.status);

        if ("OK".equalsIgnoreCase(resp.status) && resp.payload.length > 0) {
            String outFile = buildImageOutputName(filename, operation);
            Files.write(Path.of(outFile), resp.payload);
            System.out.println("Saved result to: " + outFile);
        } else if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // BASE64
    // =========================

    private static void runBase64(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err
                    .println("Usage: java cluster.TestClient base64 <serverHost> <serverPort> <encode|decode> <text>");
            return;
        }

        String operation = args[3].toUpperCase();
        String text = args[4];

        String request = "RUN|BASE64|" + operation + "|";
        byte[] payload = text.getBytes(StandardCharsets.UTF_8);

        System.out.println("Sending BASE64 request through server...");
        System.out.println("Operation: " + operation);

        ServerResponse resp = sendBinaryRequest(host, port, request, payload);

        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // COMPRESSION
    // =========================

    private static void runCompression(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: java cluster.TestClient compression <serverHost> <serverPort> <compress|decompress> <text>");
            return;
        }

        String operation = args[3].toLowerCase();
        String text = args[4];

        String request = "RUN|compression|" + operation + "|";
        byte[] payload = text.getBytes(StandardCharsets.UTF_8);

        System.out.println("Sending compression request through server...");
        System.out.println("Operation: " + operation);

        ServerResponse resp = sendBinaryRequest(host, port, request, payload);

        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // HMAC
    // =========================

    private static void runHmac(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                    "Usage: java cluster.TestClient hmac <serverHost> <serverPort> verify <message> [signature]");
            return;
        }

        String operation = args[3].toUpperCase();
        if (!"VERIFY".equals(operation)) {
            System.err.println("HMAC currently supports only: verify");
            return;
        }

        String message = args[4];
        String signature = (args.length >= 6) ? args[5] : createHmac(message);

        String request = "RUN|hmac_verify|VERIFY|" + signature;
        byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        System.out.println("Sending HMAC request through server...");
        System.out.println("Message: " + message);
        System.out.println("Signature: " + signature);

        ServerResponse resp = sendBinaryRequest(host, port, request, payload);

        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // CORE SERVER CALL
    // =========================

    private static ServerResponse sendBinaryRequest(String host, int port, String request, byte[] payload)
            throws Exception {
        try (Socket socket = new Socket(host, port);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF(request);
            if (!"LIST".equalsIgnoreCase(request)) {
                out.writeLong(payload.length);
                if (payload.length > 0) {
                    out.write(payload);
                }
            }
            out.flush();

            String status = in.readUTF();
            long length = in.readLong();

            if (length < 0 || length > Integer.MAX_VALUE) {
                throw new IOException("Invalid response length: " + length);
            }

            byte[] result = new byte[(int) length];
            if (length > 0) {
                in.readFully(result);
            }

            return new ServerResponse(status, result);
        }
    }

    // =========================
    // HELPERS
    // =========================

    private static String buildImageOutputName(String filename, String operation) {
        int slash = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
        String justName = slash >= 0 ? filename.substring(slash + 1) : filename;

        int dot = justName.lastIndexOf('.');
        if (dot > 0) {
            return justName.substring(0, dot) + "_" + operation.toLowerCase() + justName.substring(dot);
        }
        return justName + "_" + operation.toLowerCase() + ".png";
    }

    private static String createHmac(String message) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec keySpec = new SecretKeySpec(HMAC_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        mac.init(keySpec);

        byte[] raw = mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : raw) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java cluster.TestClient list <serverHost> <serverPort>");
        System.out.println(
                "  java cluster.TestClient csv <serverHost> <serverPort> <all|mean|median|min|max|stddev> <file.csv>");
        System.out.println(
                "  java cluster.TestClient image <serverHost> <serverPort> <grayscale|resize|rotate|thumbnail> <imageFile> [params]");
        System.out.println("  java cluster.TestClient base64 <serverHost> <serverPort> <encode|decode> <text>");
        System.out.println(
                "  java cluster.TestClient compression <serverHost> <serverPort> <compress|decompress> <text>");
        System.out.println("  java cluster.TestClient hmac <serverHost> <serverPort> verify <message> [signature]");
    }

    private static class ServerResponse {
        String status;
        byte[] payload;

        ServerResponse(String status, byte[] payload) {
            this.status = status;
            this.payload = payload;
        }
    }
}