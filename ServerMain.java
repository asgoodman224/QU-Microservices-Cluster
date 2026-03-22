import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerMain {

    // Server ports
    private static int SERVER_TCP_PORT;
    private static int SERVER_UDP_PORT;

    private static final long DEAD_AFTER_MS = 120_000;
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int SOCKET_TIMEOUT_MS = 30_000;

    private static final ConcurrentHashMap<String, NodeInfo> nodesById = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicInteger> rrCounters = new ConcurrentHashMap<>();

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

        SERVER_TCP_PORT = Integer.parseInt(config.getProperty("server.tcp.port"));
        SERVER_UDP_PORT = Integer.parseInt(config.getProperty("server.udp.port"));
        System.out.println("QU Cluster Server starting...");
        System.out.println("TCP (clients): " + SERVER_TCP_PORT);
        System.out.println("UDP (heartbeats): " + SERVER_UDP_PORT);

        Thread hbThread = new Thread(ServerMain::runHeartbeatListener, "heartbeat-listener");
        hbThread.setDaemon(true);
        hbThread.start();

        Thread reaperThread = new Thread(ServerMain::runDeadNodeReaper, "dead-node-reaper");
        reaperThread.setDaemon(true);
        reaperThread.start();

        runClientTcpServer();
    }

    // =========================
    // UDP HEARTBEATS
    // =========================

    private static void runHeartbeatListener() {
        try (DatagramSocket socket = new DatagramSocket(SERVER_UDP_PORT)) {
            byte[] buf = new byte[4096];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                String msg = new String(
                        packet.getData(),
                        packet.getOffset(),
                        packet.getLength(),
                        StandardCharsets.UTF_8).trim();

                String nodeId;
                String service;
                int tcpPort;

                if (msg.startsWith("{")) {
                    // JSON heartbeat format
                    String type = extractJsonValue(msg, "type");
                    service = extractJsonValue(msg, "service");
                    String portStr = extractJsonValue(msg, "port");

                    if (!"heartbeat".equalsIgnoreCase(type) || service.isEmpty() || portStr.isEmpty()) {
                        System.out.println("Ignoring bad JSON heartbeat: " + msg);
                        continue;
                    }

                    try {
                        tcpPort = Integer.parseInt(portStr);
                    } catch (NumberFormatException e) {
                        System.out.println("Ignoring JSON heartbeat with bad port: " + msg);
                        continue;
                    }

                    nodeId = service + "_" + packet.getAddress().getHostAddress() + "_" + tcpPort;

                } else {
                    // Pipe heartbeat format
                    String[] parts = msg.split("\\|");
                    if (parts.length < 4 || !"HEARTBEAT".equals(parts[0])) {
                        System.out.println("Ignoring bad heartbeat: " + msg);
                        continue;
                    }

                    nodeId = parts[1];
                    service = parts[2];

                    try {
                        tcpPort = Integer.parseInt(parts[3]);
                    } catch (NumberFormatException e) {
                        System.out.println("Ignoring heartbeat with bad port: " + msg);
                        continue;
                    }
                }

                String ip = packet.getAddress().getHostAddress();
                long now = System.currentTimeMillis();

                NodeInfo info = nodesById.compute(nodeId, (k, existing) -> {
                    NodeInfo ni = (existing == null) ? new NodeInfo() : existing;
                    ni.nodeId = nodeId;
                    ni.service = service;
                    ni.ip = ip;
                    ni.tcpPort = tcpPort;
                    ni.lastSeenMs = now;
                    return ni;
                });

                System.out.println("HB " + info.nodeId
                        + " service=" + info.service
                        + " at " + info.ip + ":" + info.tcpPort
                        + " lastSeen=" + Instant.ofEpochMilli(info.lastSeenMs));
            }
        } catch (Exception e) {
            System.err.println("Heartbeat listener crashed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runDeadNodeReaper() {
        while (true) {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                System.err.println("Reaper error: " + e.getMessage());
            }
        }
    }

    // =========================
    // CLIENT TCP SERVER
    // =========================

    private static void runClientTcpServer() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(SERVER_TCP_PORT)) {
            System.out.println("Client TCP server listening on " + SERVER_TCP_PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket), "client-handler").start();
            }
        }
    }

    private static void handleClient(Socket clientSocket) {
        String clientAddr = clientSocket.getInetAddress().getHostAddress();
        System.out.println("Client connected: " + clientAddr);

        try {
            clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);

            BufferedInputStream bis = new BufferedInputStream(clientSocket.getInputStream());
            BufferedOutputStream bos = new BufferedOutputStream(clientSocket.getOutputStream());

            bis.mark(4);
            int firstByte = bis.read();
            if (firstByte == -1) {
                clientSocket.close();
                return;
            }
            bis.reset();

            // Binary client (DataOutputStream.writeUTF usually begins with 0 length-byte)
            if (firstByte == 0) {
                handleBinaryClient(bis, bos);
            } else {
                handleTextClient(bis, bos);
            }

        } catch (Exception e) {
            System.err.println("Client handler error (" + clientAddr + "): " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (Exception ignored) {
            }
            System.out.println("Client disconnected: " + clientAddr);
        }
    }

    // =========================
    // TEXT MODE
    // =========================

    private static void handleTextClient(InputStream inStream, OutputStream outStream) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8));

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty())
                continue;

            if ("LIST".equalsIgnoreCase(line)) {
                out.write("OK|" + buildServiceListing());
                out.newLine();
                out.flush();
                continue;
            }

            if (line.startsWith("RUN|")) {
                String resp = handleTextRun(line);
                out.write(resp);
                out.newLine();
                out.flush();
                continue;
            }

            out.write("ERROR|Unknown command");
            out.newLine();
            out.flush();
        }
    }

    private static String handleTextRun(String line) {
        String[] parts = line.split("\\|", 4);
        if (parts.length < 4) {
            return "ERROR|Bad RUN format. Use RUN|SERVICE|OP|DATA";
        }

        String service = parts[1].trim();
        String op = parts[2].trim();

        NodeInfo target = pickAliveNodeForService(service);
        if (target == null) {
            return "ERROR|No alive node found for service: " + service;
        }

        try {
            // Base64 node: OP|DATA
            if ("BASE64".equalsIgnoreCase(service)) {
                String data = parts[3];
                String snReq = op.toUpperCase(Locale.ROOT) + "|" + data;
                String snResp = callTextServiceNode(target, snReq);
                return snResp.startsWith("ERROR|") ? snResp : "OK|" + snResp;
            }

            // Compression node: {"action":"compress","data":"..."}
            if ("compression".equalsIgnoreCase(service)) {
                String data = parts[3];
                String jsonReq = "{\"action\":\"" + escapeJson(op.toLowerCase(Locale.ROOT)) +
                        "\",\"data\":\"" + escapeJson(data) + "\"}";
                String snResp = callTextServiceNode(target, jsonReq);
                return "OK|" + snResp;
            }

            // HMAC node: RUN|hmac_verify|VERIFY|message|signature
            if ("hmac_verify".equalsIgnoreCase(service)) {
                String[] hmacParts = line.split("\\|", 5);
                if (hmacParts.length < 5) {
                    return "ERROR|Bad HMAC format. Use RUN|hmac_verify|VERIFY|message|signature";
                }

                String message = hmacParts[3];
                String signature = hmacParts[4];

                String jsonReq = "{\"message\":\"" + escapeJson(message) +
                        "\",\"signature\":\"" + escapeJson(signature) + "\"}";
                String snResp = callTextServiceNode(target, jsonReq);
                return "OK|" + snResp;
            }

            // CSV / IMAGE should use binary client mode instead
            return "ERROR|This service should use binary client mode: " + service;

        } catch (Exception e) {
            System.err.println("Text RUN failed for service " + service + ": " + e.getMessage());
            return "ERROR|Service node failed: " + target.nodeId;
        }
    }

    // =========================
    // BINARY MODE
    // =========================

    private static void handleBinaryClient(InputStream inStream, OutputStream outStream) throws Exception {
        DataInputStream in = new DataInputStream(inStream);
        DataOutputStream out = new DataOutputStream(outStream);

        /*
         * Binary client protocol:
         * writeUTF("LIST")
         * or
         * writeUTF("RUN|SERVICE|OP|PARAMS")
         * writeLong(payload.length)
         * write(payload)
         *
         * Response:
         * writeUTF(status)
         * writeLong(result.length)
         * write(result)
         */

        String request = in.readUTF();

        if ("LIST".equalsIgnoreCase(request)) {
            byte[] listing = buildServiceListing().getBytes(StandardCharsets.UTF_8);
            out.writeUTF("OK");
            out.writeLong(listing.length);
            out.write(listing);
            out.flush();
            return;
        }

        String[] parts = request.split("\\|", 4);
        if (parts.length < 3 || !"RUN".equalsIgnoreCase(parts[0])) {
            out.writeUTF("ERROR");
            out.writeLong(0);
            out.flush();
            return;
        }

        String service = parts[1].trim();
        String op = parts[2].trim();
        String params = (parts.length == 4) ? parts[3] : "";

        long payloadLen = in.readLong();
        if (payloadLen < 0 || payloadLen > Integer.MAX_VALUE) {
            out.writeUTF("ERROR");
            out.writeLong(0);
            out.flush();
            return;
        }

        byte[] payload = new byte[(int) payloadLen];
        in.readFully(payload);

        NodeInfo target = pickAliveNodeForService(service);
        if (target == null) {
            out.writeUTF("ERROR");
            out.writeLong(0);
            out.flush();
            return;
        }

        try {
            // CSV and IMAGE use DataInput/DataOutput protocol
            if ("CSV_STATS".equalsIgnoreCase(service) || "IMAGE_TRANSFORM".equalsIgnoreCase(service)) {
                String nodeRequest = op.toUpperCase(Locale.ROOT) + "|" + params;
                BinaryNodeResponse resp = callBinaryServiceNode(target, nodeRequest, payload);

                out.writeUTF(resp.status);
                out.writeLong(resp.payload.length);
                if (resp.payload.length > 0)
                    out.write(resp.payload);
                out.flush();
                return;
            }

            // Allow binary clients to call compression/hmac/base64 too if they want
            if ("BASE64".equalsIgnoreCase(service)) {
                String text = new String(payload, StandardCharsets.UTF_8);
                String snReq = op.toUpperCase(Locale.ROOT) + "|" + text;
                String snResp = callTextServiceNode(target, snReq);
                byte[] result = snResp.getBytes(StandardCharsets.UTF_8);

                out.writeUTF(snResp.startsWith("ERROR|") ? "ERROR" : "OK");
                out.writeLong(result.length);
                out.write(result);
                out.flush();
                return;
            }

            if ("compression".equalsIgnoreCase(service)) {
                String text = new String(payload, StandardCharsets.UTF_8);
                String jsonReq = "{\"action\":\"" + escapeJson(op.toLowerCase(Locale.ROOT)) +
                        "\",\"data\":\"" + escapeJson(text) + "\"}";
                String snResp = callTextServiceNode(target, jsonReq);
                byte[] result = snResp.getBytes(StandardCharsets.UTF_8);

                out.writeUTF("OK");
                out.writeLong(result.length);
                out.write(result);
                out.flush();
                return;
            }

            if ("hmac_verify".equalsIgnoreCase(service)) {
                // params should be the signature, payload is the message
                String message = new String(payload, StandardCharsets.UTF_8);
                String jsonReq = "{\"message\":\"" + escapeJson(message) +
                        "\",\"signature\":\"" + escapeJson(params) + "\"}";
                String snResp = callTextServiceNode(target, jsonReq);
                byte[] result = snResp.getBytes(StandardCharsets.UTF_8);

                out.writeUTF("OK");
                out.writeLong(result.length);
                out.write(result);
                out.flush();
                return;
            }

            out.writeUTF("ERROR");
            out.writeLong(0);
            out.flush();

        } catch (Exception e) {
            System.err.println("Binary RUN failed for service " + service + ": " + e.getMessage());
            out.writeUTF("ERROR");
            out.writeLong(0);
            out.flush();
        }
    }

    // =========================
    // NODE CALLS
    // =========================

    private static String callTextServiceNode(NodeInfo ni, String requestLine) throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(ni.ip, ni.tcpPort), CONNECT_TIMEOUT_MS);
            s.setSoTimeout(SOCKET_TIMEOUT_MS);

            try (
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    BufferedWriter out = new BufferedWriter(
                            new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))) {
                out.write(requestLine);
                out.newLine();
                out.flush();

                String resp = in.readLine();
                if (resp == null)
                    throw new IOException("Node closed connection without response");
                return resp.trim();
            }
        }
    }

    private static BinaryNodeResponse callBinaryServiceNode(NodeInfo ni, String request, byte[] payload)
            throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(ni.ip, ni.tcpPort), CONNECT_TIMEOUT_MS);
            s.setSoTimeout(SOCKET_TIMEOUT_MS);

            try (
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream())) {
                out.writeUTF(request);
                out.writeLong(payload.length);
                out.write(payload);
                out.flush();

                String status = in.readUTF();
                long resultLen = in.readLong();
                if (resultLen < 0 || resultLen > Integer.MAX_VALUE) {
                    throw new IOException("Bad result length: " + resultLen);
                }

                byte[] result = new byte[(int) resultLen];
                if (resultLen > 0)
                    in.readFully(result);

                return new BinaryNodeResponse(status, result);
            }
        }
    }

    // =========================
    // HELPERS
    // =========================

    private static NodeInfo pickAliveNodeForService(String service) {
        long now = System.currentTimeMillis();
        List<NodeInfo> alive = new ArrayList<>();

        for (NodeInfo ni : nodesById.values()) {
            if (service.equalsIgnoreCase(ni.service) && (now - ni.lastSeenMs) <= DEAD_AFTER_MS) {
                alive.add(ni);
            }
        }

        if (alive.isEmpty())
            return null;

        String key = service.toUpperCase(Locale.ROOT);
        rrCounters.putIfAbsent(key, new AtomicInteger(0));
        int idx = Math.floorMod(rrCounters.get(key).getAndIncrement(), alive.size());
        return alive.get(idx);
    }

    private static String buildServiceListing() {
        long now = System.currentTimeMillis();
        Map<String, List<NodeInfo>> byService = new TreeMap<>();

        for (NodeInfo ni : nodesById.values()) {
            if ((now - ni.lastSeenMs) <= DEAD_AFTER_MS) {
                byService.computeIfAbsent(ni.service, k -> new ArrayList<>()).add(ni);
            }
        }

        if (byService.isEmpty())
            return "No services available";

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<NodeInfo>> e : byService.entrySet()) {
            sb.append(e.getKey()).append(": ");
            List<NodeInfo> list = e.getValue();

            for (int i = 0; i < list.size(); i++) {
                NodeInfo ni = list.get(i);
                sb.append(ni.nodeId).append("@").append(ni.ip).append(":").append(ni.tcpPort);
                if (i < list.size() - 1)
                    sb.append(", ");
            }
            sb.append(" ; ");
        }
        return sb.toString();
    }

    private static String extractJsonValue(String json, String key) {
        String stringPattern = "\"" + key + "\":\"";
        int start = json.indexOf(stringPattern);

        if (start != -1) {
            start += stringPattern.length();
            int end = json.indexOf("\"", start);
            if (end != -1)
                return json.substring(start, end);
        }

        String numberPattern = "\"" + key + "\":";
        start = json.indexOf(numberPattern);
        if (start != -1) {
            start += numberPattern.length();
            int end = start;
            while (end < json.length() && Character.isDigit(json.charAt(end))) {
                end++;
            }
            return json.substring(start, end);
        }

        return "";
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static class NodeInfo {
        String nodeId;
        String service;
        String ip;
        int tcpPort;
        long lastSeenMs;
    }

    private static class BinaryNodeResponse {
        String status;
        byte[] payload;

        BinaryNodeResponse(String status, byte[] payload) {
            this.status = status;
            this.payload = payload;
        }
    }
}
