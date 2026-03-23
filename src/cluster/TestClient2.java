package cluster;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Unified test client for QU Cluster Server.
 *
 * TWO MODES:
 *
 *   1) INTERACTIVE (persistent connection with live push updates):
 *      java TestClient2 interactive 172.31.44.92 5100
 *
 *   2) ONE-SHOT (single command, then disconnect):
 *      java TestClient2 list 172.31.44.92 5100
 *      java TestClient2 csv 172.31.44.92 5100 all data.csv
 *      java TestClient2 image 172.31.44.92 5100 grayscale photo.png
 *      java TestClient2 image 172.31.44.92 5100 resize photo.png 400x300
 *      java TestClient2 base64 172.31.44.92 5100 encode photo.png
 *      java TestClient2 base64 172.31.44.92 5100 encode "hello world"
 *      java TestClient2 compression 172.31.44.92 5100 compress document.pdf
 *      java TestClient2 compression 172.31.44.92 5100 compress "hello world"
 *      java TestClient2 hmac 172.31.44.92 5100 sign photo.png
 *      java TestClient2 hmac 172.31.44.92 5100 verify "hello world" <signature>
 *
 * For base64, compression, and hmac: if the argument is an existing file path,
 * the file contents are sent. Otherwise it is treated as a text string.
 */
public class TestClient2 {

    private static final String HMAC_SECRET = "this_is_a_secret_key";

    // For interactive mode
    private static final LinkedBlockingQueue<ServerMessage> responseQueue = new LinkedBlockingQueue<>();
    private static DataOutputStream sharedOut;
    private static volatile boolean connected = true;

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
            if ("interactive".equals(mode)) {
                runInteractive(serverHost, serverPort);
            } else {
                runOneShot(mode, serverHost, serverPort, args);
            }
        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // =================================================================
    // INTERACTIVE (PERSISTENT) MODE
    // =================================================================

    private static void runInteractive(String host, int port) throws Exception {
        System.out.println("Connecting to " + host + ":" + port + "...");
        Socket socket = new Socket(host, port);
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        sharedOut = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

        System.out.println("Connected!");

        // Reader thread: reads all messages from server (RESPONSE and PUSH)
        Thread readerThread = new Thread(() -> runReader(in), "server-reader");
        readerThread.setDaemon(true);
        readerThread.start();

        // Subscribe for push notifications — server will immediately send SERVICE_LIST
        synchronized (sharedOut) {
            sharedOut.writeUTF("SUBSCRIBE");
            sharedOut.flush();
        }

        System.out.println();
        System.out.println("=== INTERACTIVE MODE ===");
        System.out.println("Commands:");
        System.out.println("  list");
        System.out.println("  csv <all|mean|median|min|max|stddev> <file.csv>");
        System.out.println("  image <grayscale|resize|rotate|thumbnail> <file> [params]");
        System.out.println("  base64 <encode|decode> <file_or_text>");
        System.out.println("  compression <compress|decompress> <file_or_text>");
        System.out.println("  hmac <sign|verify> <file_or_text> [signature]");
        System.out.println("  quit");
        System.out.println("========================");
        System.out.println();

        // Read commands from stdin
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        while (connected) {
            System.out.print("> ");
            String line = stdin.readLine();
            if (line == null) break;
            line = line.trim();
            if (line.isEmpty()) continue;

            if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line)) {
                synchronized (sharedOut) {
                    sharedOut.writeUTF("QUIT");
                    sharedOut.flush();
                }
                break;
            }

            try {
                handleInteractiveCommand(line);
            } catch (Exception e) {
                System.err.println("Command error: " + e.getMessage());
            }
        }

        connected = false;
        socket.close();
        System.out.println("Disconnected.");
    }

    /**
     * Background reader thread. Reads every message from the server and routes it:
     * - PUSH messages are printed immediately to the console.
     * - RESPONSE messages are placed on the responseQueue for the main thread.
     */
    private static void runReader(DataInputStream in) {
        try {
            while (connected) {
                String messageType = in.readUTF();        // "RESPONSE" or "PUSH"
                String statusOrType = in.readUTF();       // "OK"/"ERROR" or "SERVICE_LIST"
                long length = in.readLong();

                byte[] data = new byte[(int) length];
                if (length > 0) in.readFully(data);

                if ("PUSH".equals(messageType)) {
                    System.out.println();
                    System.out.println("[PUSH " + statusOrType + "] "
                            + new String(data, StandardCharsets.UTF_8));
                    System.out.print("> ");  // Re-print prompt
                } else {
                    responseQueue.put(new ServerMessage(statusOrType, data));
                }
            }
        } catch (EOFException e) {
            System.out.println("\n[Reader] Server closed connection.");
            connected = false;
        } catch (Exception e) {
            if (connected) {
                System.err.println("\n[Reader] Error: " + e.getMessage());
                connected = false;
            }
        }
    }

    /**
     * Parses a command from stdin and sends it over the persistent connection.
     */
    private static void handleInteractiveCommand(String line) throws Exception {
        String[] tokens = line.split("\\s+");
        String cmd = tokens[0].toLowerCase();

        switch (cmd) {
            case "list":
                sendInteractiveRequest("LIST", null, -1);
                break;

            case "csv":
                if (tokens.length < 3) { System.out.println("Usage: csv <op> <file>"); return; }
                byte[] csvBytes = Files.readAllBytes(Path.of(tokens[2]));
                sendInteractiveRequest("RUN|CSV_STATS|" + tokens[1].toUpperCase() + "|", csvBytes, csvBytes.length);
                break;

            case "image":
                if (tokens.length < 3) { System.out.println("Usage: image <op> <file> [params]"); return; }
                String imgParams = tokens.length >= 4 ? tokens[3] : "";
                byte[] imgBytes = Files.readAllBytes(Path.of(tokens[2]));
                sendInteractiveRequest("RUN|IMAGE_TRANSFORM|" + tokens[1].toUpperCase() + "|" + imgParams, imgBytes, imgBytes.length);
                break;

            case "base64":
                if (tokens.length < 3) { System.out.println("Usage: base64 <encode|decode> <file_or_text>"); return; }
                byte[] b64Data = readFileOrText(tokens[2]);
                sendInteractiveRequest("RUN|BASE64|" + tokens[1].toUpperCase() + "|", b64Data, b64Data.length);
                break;

            case "compression":
                if (tokens.length < 3) { System.out.println("Usage: compression <compress|decompress> <file_or_text>"); return; }
                byte[] compData = readFileOrText(tokens[2]);
                sendInteractiveRequest("RUN|compression|" + tokens[1].toUpperCase() + "|", compData, compData.length);
                break;

            case "hmac":
                if (tokens.length < 3) { System.out.println("Usage: hmac <sign|verify> <file_or_text> [signature]"); return; }
                String hmacOp = tokens[1].toUpperCase();
                byte[] hmacData = readFileOrText(tokens[2]);
                String hmacParams = "";
                if ("VERIFY".equals(hmacOp)) {
                    hmacParams = tokens.length >= 4 ? tokens[3] : createHmac(hmacData);
                }
                sendInteractiveRequest("RUN|hmac_verify|" + hmacOp + "|" + hmacParams, hmacData, hmacData.length);
                break;

            default:
                System.out.println("Unknown command: " + cmd);
        }
    }

    /**
     * Sends a request over the persistent connection and waits for the response.
     */
    private static void sendInteractiveRequest(String request, byte[] payload, long payloadLen) throws Exception {
        responseQueue.clear();

        synchronized (sharedOut) {
            sharedOut.writeUTF(request);
            if (payloadLen >= 0 && payload != null) {
                sharedOut.writeLong(payloadLen);
                if (payloadLen > 0) sharedOut.write(payload);
            }
            sharedOut.flush();
        }

        ServerMessage resp = responseQueue.poll(30, TimeUnit.SECONDS);

        if (resp == null) {
            System.out.println("Timed out waiting for response.");
            return;
        }

        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            if (isPrintable(resp.payload)) {
                System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
            } else {
                System.out.println("Received " + resp.payload.length + " bytes of binary data.");
                System.out.println("(Use one-shot mode to save results to a file.)");
            }
        }
    }

    // =================================================================
    // ONE-SHOT MODE
    // =================================================================

    private static void runOneShot(String mode, String host, int port, String[] args) throws Exception {
        switch (mode) {
            case "list":
                runList(host, port);
                break;
            case "csv":
                runCsv(host, port, args);
                break;
            case "image":
                runImage(host, port, args);
                break;
            case "base64":
                runBase64(host, port, args);
                break;
            case "compression":
                runCompression(host, port, args);
                break;
            case "hmac":
                runHmac(host, port, args);
                break;
            default:
                System.err.println("Unknown mode: " + mode);
                printUsage();
        }
    }

    // --- LIST ---
    private static void runList(String host, int port) throws Exception {
        ServerResponse resp = sendOneShotRequest(host, port, "LIST", new byte[0]);
        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // --- CSV ---
    private static void runCsv(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java TestClient2 csv <host> <port> <all|mean|...> <file.csv>");
            return;
        }
        String operation = args[3].toUpperCase();
        String filename = args[4];
        byte[] csvBytes = Files.readAllBytes(Path.of(filename));

        System.out.println("Sending CSV request through server...");
        System.out.println("Operation: " + operation);
        System.out.println("File: " + filename + " (" + csvBytes.length + " bytes)");

        ServerResponse resp = sendOneShotRequest(host, port, "RUN|CSV_STATS|" + operation + "|", csvBytes);
        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // --- IMAGE ---
    private static void runImage(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java TestClient2 image <host> <port> <op> <file> [params]");
            return;
        }
        String operation = args[3].toUpperCase();
        String filename = args[4];
        String params = args.length >= 6 ? args[5] : "";
        byte[] imageBytes = Files.readAllBytes(Path.of(filename));

        System.out.println("Sending image request through server...");
        System.out.println("Operation: " + operation + (params.isBlank() ? "" : " (" + params + ")"));
        System.out.println("File: " + filename + " (" + imageBytes.length + " bytes)");

        ServerResponse resp = sendOneShotRequest(host, port,
                "RUN|IMAGE_TRANSFORM|" + operation + "|" + params, imageBytes);
        System.out.println("Status: " + resp.status);

        if ("OK".equalsIgnoreCase(resp.status) && resp.payload.length > 0) {
            String outFile = buildOutputName(filename, operation, null);
            Files.write(Path.of(outFile), resp.payload);
            System.out.println("Saved result to: " + outFile);
        } else if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // --- BASE64 (accepts files) ---
    private static void runBase64(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java TestClient2 base64 <host> <port> <encode|decode> <file_or_text>");
            return;
        }
        String operation = args[3].toUpperCase();
        String input = args[4];
        byte[] payload = readFileOrText(input);
        boolean isFile = Files.exists(Path.of(input));

        System.out.println("Sending BASE64 request through server...");
        System.out.println("Operation: " + operation);
        System.out.println(isFile ? "File: " + input + " (" + payload.length + " bytes)" : "Text input");

        ServerResponse resp = sendOneShotRequest(host, port, "RUN|BASE64|" + operation + "|", payload);
        System.out.println("Status: " + resp.status);

        if ("OK".equalsIgnoreCase(resp.status) && resp.payload.length > 0) {
            if (isPrintable(resp.payload)) {
                System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
                if (isFile) {
                    String outFile = buildOutputName(input, operation, ".txt");
                    Files.write(Path.of(outFile), resp.payload);
                    System.out.println("Also saved to: " + outFile);
                }
            } else {
                String outFile = buildOutputName(isFile ? input : "output", operation, null);
                Files.write(Path.of(outFile), resp.payload);
                System.out.println("Saved " + resp.payload.length + " bytes to: " + outFile);
            }
        } else if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // --- COMPRESSION (accepts files) ---
    private static void runCompression(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java TestClient2 compression <host> <port> <compress|decompress> <file_or_text>");
            return;
        }
        String operation = args[3].toUpperCase();
        String input = args[4];
        byte[] payload = readFileOrText(input);
        boolean isFile = Files.exists(Path.of(input));

        System.out.println("Sending compression request through server...");
        System.out.println("Operation: " + operation);
        System.out.println(isFile ? "File: " + input + " (" + payload.length + " bytes)" : "Text input");

        ServerResponse resp = sendOneShotRequest(host, port, "RUN|compression|" + operation + "|", payload);
        System.out.println("Status: " + resp.status);

        if ("OK".equalsIgnoreCase(resp.status) && resp.payload.length > 0) {
            if ("COMPRESS".equals(operation)) {
                String outFile = buildOutputName(isFile ? input : "output", "compressed", ".gz");
                Files.write(Path.of(outFile), resp.payload);
                System.out.println("Compressed " + payload.length + " -> " + resp.payload.length
                        + " bytes (" + (100 - (resp.payload.length * 100 / Math.max(payload.length, 1))) + "% reduction)");
                System.out.println("Saved to: " + outFile);
            } else {
                if (isPrintable(resp.payload)) {
                    System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
                }
                if (isFile || !isPrintable(resp.payload)) {
                    String outFile = buildOutputName(isFile ? input : "output", "decompressed", null);
                    Files.write(Path.of(outFile), resp.payload);
                    System.out.println("Saved " + resp.payload.length + " bytes to: " + outFile);
                }
            }
        } else if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // --- HMAC (accepts files, supports sign + verify) ---
    private static void runHmac(String host, int port, String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: java TestClient2 hmac <host> <port> <sign|verify> <file_or_text> [signature]");
            return;
        }
        String operation = args[3].toUpperCase();
        String input = args[4];
        byte[] payload = readFileOrText(input);
        boolean isFile = Files.exists(Path.of(input));

        String params = "";
        if ("VERIFY".equals(operation)) {
            params = (args.length >= 6) ? args[5] : createHmac(payload);
        }

        System.out.println("Sending HMAC request through server...");
        System.out.println("Operation: " + operation);
        System.out.println(isFile ? "File: " + input + " (" + payload.length + " bytes)" : "Text input");
        if ("VERIFY".equals(operation)) {
            System.out.println("Signature: " + params);
        }

        ServerResponse resp = sendOneShotRequest(host, port,
                "RUN|hmac_verify|" + operation + "|" + params, payload);
        System.out.println("Status: " + resp.status);
        if (resp.payload.length > 0) {
            System.out.println(new String(resp.payload, StandardCharsets.UTF_8));
        }
    }

    // =========================
    // ONE-SHOT PROTOCOL
    // =========================

    /**
     * Sends a single request and reads one response.
     * Handles the RESPONSE-tagged protocol from the persistent server.
     */
    private static ServerResponse sendOneShotRequest(String host, int port, String request, byte[] payload)
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

            // Server now tags messages: first field is "RESPONSE"
            String messageType = in.readUTF();  // "RESPONSE"
            String status = in.readUTF();       // "OK" or "ERROR"
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

    /**
     * If the argument is a path to an existing file, read the file.
     * Otherwise treat it as a text string.
     */
    private static byte[] readFileOrText(String input) throws IOException {
        Path path = Path.of(input);
        if (Files.exists(path) && Files.isRegularFile(path)) {
            System.out.println("Reading file: " + input);
            return Files.readAllBytes(path);
        }
        return input.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Builds an output filename like: photo_grayscale.png or data_compressed.gz
     */
    private static String buildOutputName(String inputFilename, String operation, String forceExtension) {
        int slash = Math.max(inputFilename.lastIndexOf('/'), inputFilename.lastIndexOf('\\'));
        String justName = slash >= 0 ? inputFilename.substring(slash + 1) : inputFilename;
        int dot = justName.lastIndexOf('.');
        String baseName = dot > 0 ? justName.substring(0, dot) : justName;
        String origExt = dot > 0 ? justName.substring(dot) : "";
        String ext = (forceExtension != null) ? forceExtension : origExt;
        return baseName + "_" + operation.toLowerCase() + ext;
    }

    private static boolean isPrintable(byte[] data) {
        int sampleSize = Math.min(data.length, 200);
        int printableCount = 0;
        for (int i = 0; i < sampleSize; i++) {
            int b = data[i] & 0xFF;
            if (b >= 32 && b < 127 || b == '\n' || b == '\r' || b == '\t') {
                printableCount++;
            }
        }
        return (double) printableCount / sampleSize > 0.85;
    }

    private static String createHmac(byte[] data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec keySpec = new SecretKeySpec(HMAC_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        mac.init(keySpec);
        byte[] raw = mac.doFinal(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : raw) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static String createHmac(String message) throws Exception {
        return createHmac(message.getBytes(StandardCharsets.UTF_8));
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("  INTERACTIVE (persistent connection with live push updates):");
        System.out.println("    java TestClient2 interactive <serverHost> <serverPort>");
        System.out.println();
        System.out.println("  ONE-SHOT (single command, then disconnect):");
        System.out.println("    java TestClient2 list <serverHost> <serverPort>");
        System.out.println("    java TestClient2 csv <host> <port> <all|mean|...> <file.csv>");
        System.out.println("    java TestClient2 image <host> <port> <op> <file> [params]");
        System.out.println("    java TestClient2 base64 <host> <port> <encode|decode> <file_or_text>");
        System.out.println("    java TestClient2 compression <host> <port> <compress|decompress> <file_or_text>");
        System.out.println("    java TestClient2 hmac <host> <port> <sign|verify> <file_or_text> [signature]");
        System.out.println();
        System.out.println("  For base64/compression/hmac: if the argument is an existing file,");
        System.out.println("  the file contents are sent. Otherwise it is treated as text.");
    }

    // =========================
    // INNER CLASSES
    // =========================

    /** Message received from the server in interactive mode. */
    private static class ServerMessage {
        String status;
        byte[] payload;
        ServerMessage(String status, byte[] payload) { this.status = status; this.payload = payload; }
    }

    /** Response from a one-shot request. */
    private static class ServerResponse {
        String status;
        byte[] payload;
        ServerResponse(String status, byte[] payload) { this.status = status; this.payload = payload; }
    }
}