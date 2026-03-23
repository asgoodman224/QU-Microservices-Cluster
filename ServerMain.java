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
 
    // Track all persistent client connections for push notifications
    private static final Set<ClientConnection> connectedClients = ConcurrentHashMap.newKeySet();
 
    public static void main(String[] args) throws Exception {
 
        // Load config.properties
        Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            config.load(fis);
        } catch (IOException e) {
            System.out.println("Failed to load config.properties");
            e.printStackTrace();
            return;
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
 
                // Check if this is a brand-new node
                boolean isNewNode = !nodesById.containsKey(nodeId);
 
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
 
                // If a new service node just registered, push updated list to all clients
                if (isNewNode) {
                    System.out.println(">> New node registered: " + nodeId + " — broadcasting to clients");
                    broadcastServiceList();
                }
            }
        } catch (Exception e) {
            System.err.println("Heartbeat listener crashed: " + e.getMessage());
            e.printStackTrace();
        }
    }
 
    // Reaps dead nodes and notifies clients when the service list changes
    private static void runDeadNodeReaper() {
        while (true) {
            try {
                Thread.sleep(5_000);
 
                long now = System.currentTimeMillis();
                boolean removed = false;
 
                Iterator<Map.Entry<String, NodeInfo>> it = nodesById.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, NodeInfo> entry = it.next();
                    NodeInfo ni = entry.getValue();
 
                    if ((now - ni.lastSeenMs) > DEAD_AFTER_MS) {
                        System.out.println(">> Reaping dead node: " + ni.nodeId
                                + " (last seen " + ((now - ni.lastSeenMs) / 1000) + "s ago)");
                        it.remove();
                        removed = true;
                    }
                }
 
                if (removed) {
                    broadcastServiceList();
                }
 
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                System.err.println("Reaper error: " + e.getMessage());
            }
        }
    }
 
    // =========================
    // PUSH NOTIFICATIONS
    // =========================
 
    /**
     * Sends the current service listing to ALL connected persistent clients.
     */
    private static void broadcastServiceList() {
        String listing = buildServiceListing();
        byte[] data = listing.getBytes(StandardCharsets.UTF_8);
 
        System.out.println(">> Broadcasting service list to " + connectedClients.size() + " client(s)");
 
        for (ClientConnection cc : connectedClients) {
            try {
                cc.sendPush("SERVICE_LIST", data);
            } catch (Exception e) {
                System.out.println(">> Failed to push to client " + cc.label + ", removing");
                connectedClients.remove(cc);
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
                handleBinaryClient(clientSocket, bis, bos);
            } else {
                handleTextClient(clientSocket, bis, bos);
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
 
    private static void handleTextClient(Socket clientSocket, InputStream inStream, OutputStream outStream) throws Exception {
        clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
 
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
 
            out.write("ERROR|All services now require binary client mode. Use TestClient2.");
            out.newLine();
            out.flush();
        }
    }
 
    // =========================
    // BINARY MODE — persistent connection with push support
    // =========================
 
    /**
     * Binary protocol (persistent):
     *
     * CLIENT -> SERVER:
     *   writeUTF(request)          "LIST", "RUN|...", "SUBSCRIBE", or "QUIT"
     *   writeLong(payload.length)   (only for RUN requests)
     *   write(payload)              (only for RUN requests)
     *
     * SERVER -> CLIENT (response to a request):
     *   writeUTF("RESPONSE")
     *   writeUTF(status)            "OK" or "ERROR"
     *   writeLong(result.length)
     *   write(result)
     *
     * SERVER -> CLIENT (unsolicited push):
     *   writeUTF("PUSH")
     *   writeUTF(pushType)          e.g. "SERVICE_LIST"
     *   writeLong(data.length)
     *   write(data)
     *
     * All five services use the same binary protocol to service nodes:
     *   writeUTF("OP|params"), writeLong(len), write(data)
     *   -> readUTF(status), readLong(len), readFully(result)
     */
    private static void handleBinaryClient(Socket clientSocket, InputStream inStream, OutputStream outStream) throws Exception {
        // No socket timeout — persistent connections stay open
        clientSocket.setSoTimeout(0);
 
        DataInputStream in = new DataInputStream(inStream);
        DataOutputStream out = new DataOutputStream(outStream);
 
        String clientAddr = clientSocket.getInetAddress().getHostAddress();
        ClientConnection cc = new ClientConnection(out, clientAddr);
 
        // Loop: keep reading requests until client disconnects or sends QUIT
        while (true) {
            String request;
            try {
                request = in.readUTF();
            } catch (EOFException e) {
                // Client closed the connection cleanly
                break;
            }
 
            // ---- SUBSCRIBE: opt in to push notifications ----
            if ("SUBSCRIBE".equalsIgnoreCase(request)) {
                connectedClients.add(cc);
                System.out.println("Client " + clientAddr + " subscribed for push notifications");
 
                // Immediately send the current service list
                String listing = buildServiceListing();
                byte[] listingBytes = listing.getBytes(StandardCharsets.UTF_8);
                cc.sendPush("SERVICE_LIST", listingBytes);
                continue;
            }
 
            // ---- QUIT: client wants to disconnect ----
            if ("QUIT".equalsIgnoreCase(request)) {
                System.out.println("Client " + clientAddr + " sent QUIT");
                break;
            }
 
            // ---- LIST: request current services ----
            if ("LIST".equalsIgnoreCase(request)) {
                byte[] listing = buildServiceListing().getBytes(StandardCharsets.UTF_8);
                cc.sendResponse("OK", listing);
                continue;
            }
 
            // ---- RUN|SERVICE|OP|PARAMS: forward to a service node ----
            String[] parts = request.split("\\|", 4);
            if (parts.length < 3 || !"RUN".equalsIgnoreCase(parts[0])) {
                cc.sendResponse("ERROR", new byte[0]);
                continue;
            }
 
            String service = parts[1].trim();
            String op = parts[2].trim();
            String params = (parts.length == 4) ? parts[3] : "";
 
            long payloadLen = in.readLong();
            if (payloadLen < 0 || payloadLen > Integer.MAX_VALUE) {
                cc.sendResponse("ERROR", new byte[0]);
                continue;
            }
 
            byte[] payload = new byte[(int) payloadLen];
            in.readFully(payload);
 
            NodeInfo target = pickAliveNodeForService(service);
            if (target == null) {
                cc.sendResponse("ERROR",
                        ("No alive node for service: " + service).getBytes(StandardCharsets.UTF_8));
                continue;
            }
 
            try {
                // All 5 services use the same binary protocol
                String nodeRequest = op.toUpperCase(Locale.ROOT) + "|" + params;
                BinaryNodeResponse resp = callBinaryServiceNode(target, nodeRequest, payload);
                cc.sendResponse(resp.status, resp.payload);
 
            } catch (Exception e) {
                System.err.println("RUN failed for service " + service + ": " + e.getMessage());
                cc.sendResponse("ERROR",
                        ("Service error: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
            }
        }
 
        // Clean up on disconnect
        connectedClients.remove(cc);
    }
 
    // =========================
    // NODE CALLS
    // =========================
 
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
 
    // =========================
    // INNER CLASSES
    // =========================
 
    /**
     * Wraps a connected client's output stream with synchronized write methods.
     * Synchronization prevents interleaved bytes when multiple threads
     * (request handler + broadcast push) write to the same client at once.
     */
    private static class ClientConnection {
        final DataOutputStream out;
        final String label;
 
        ClientConnection(DataOutputStream out, String label) {
            this.out = out;
            this.label = label;
        }
 
        /** Send a tagged response to a client's request. */
        void sendResponse(String status, byte[] data) throws IOException {
            synchronized (out) {
                out.writeUTF("RESPONSE");
                out.writeUTF(status);
                out.writeLong(data.length);
                if (data.length > 0) out.write(data);
                out.flush();
            }
        }
 
        /** Send an unsolicited push notification to this client. */
        void sendPush(String pushType, byte[] data) throws IOException {
            synchronized (out) {
                out.writeUTF("PUSH");
                out.writeUTF(pushType);
                out.writeLong(data.length);
                if (data.length > 0) out.write(data);
                out.flush();
            }
        }
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
 