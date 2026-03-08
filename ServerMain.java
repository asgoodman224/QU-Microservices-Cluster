package cluster;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerMain {

    // ====== CONFIG ======
    private static final int SERVER_TCP_PORT = 5100; // clients connect here
    private static final int SERVER_UDP_PORT = 5001; // SN heartbeats arrive here

    private static final long DEAD_AFTER_MS = 120_000; // 120 seconds
    private static final int SN_TCP_TIMEOUT_MS = 10_000;

    // ====================

    /**
     * Registry of nodes by nodeId.
     */
    private static final ConcurrentHashMap<String, NodeInfo> nodesById = new ConcurrentHashMap<>();

    /**
     * Round-robin counters per service.
     */
    private static final ConcurrentHashMap<String, AtomicInteger> rrCounters = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("QU Cluster Server starting...");
        System.out.println("TCP (clients): " + SERVER_TCP_PORT);
        System.out.println("UDP (heartbeats): " + SERVER_UDP_PORT);

        // 1) Start UDP heartbeat listener
        Thread hbListener = new Thread(ServerMain::runHeartbeatListener, "heartbeat-listener");
        hbListener.setDaemon(true);
        hbListener.start();

        // 2) Start reaper thread to expire dead nodes
        Thread reaper = new Thread(ServerMain::runDeadNodeReaper, "dead-node-reaper");
        reaper.setDaemon(true);
        reaper.start();

        // 3) Start TCP server for clients
        runClientTcpServer();
    }

    // ================= UDP HEARTBEATS =================

    private static void runHeartbeatListener() {
        try (DatagramSocket socket = new DatagramSocket(SERVER_UDP_PORT)) {

            byte[] buf = new byte[4096];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);

                String msg = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8).trim();

                // Expected: HEARTBEAT|nodeId|serviceName|snTcpPort
                String[] parts = msg.split("\\|");
                if (parts.length < 4 || !"HEARTBEAT".equals(parts[0])) {
                    System.out.println("Ignoring UDP: " + msg);
                    continue;
                }

                String nodeId = parts[1];
                String service = parts[2];
                int snTcpPort;

                try {
                    snTcpPort = Integer.parseInt(parts[3]);
                } catch (NumberFormatException e) {
                    System.out.println("Bad heartbeat port: " + msg);
                    continue;
                }

                String snIp = packet.getAddress().getHostAddress();
                long now = System.currentTimeMillis();

                NodeInfo info = nodesById.compute(nodeId, (k, existing) -> {
                    NodeInfo ni = (existing == null) ? new NodeInfo() : existing;
                    ni.nodeId = nodeId;
                    ni.service = service;
                    ni.ip = snIp;
                    ni.tcpPort = snTcpPort;
                    ni.lastSeenMs = now;
                    return ni;
                });

                // Log occasionally; you can reduce spam if you want
                System.out.println("HB " + nodeId + " service=" + info.service + " at " + info.ip + ":" + info.tcpPort +
                        " lastSeen=" + Instant.ofEpochMilli(info.lastSeenMs));
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

                long now = System.currentTimeMillis();
                for (Map.Entry<String, NodeInfo> entry : nodesById.entrySet()) {
                    NodeInfo ni = entry.getValue();
                    boolean alive = (now - ni.lastSeenMs) <= DEAD_AFTER_MS;
                    if (!alive) {
                        // Keep it in map but mark dead (or remove it entirely—either is fine)
                        // Here we keep it so you can still see it if you print debug state.
                    }
                }

            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                System.err.println("Reaper error: " + e.getMessage());
            }
        }
    }

    // ================= CLIENT TCP SERVER =================

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

        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8))
        ) {
            /*
             * Client protocol (line-based):
             * LIST
             *
             * RUN|BASE64|ENCODE|<text...>
             * RUN|BASE64|DECODE|<base64...>
             *
             * Response:
             * OK|<data...>
             * or ERROR|<message>
             */

            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if ("LIST".equalsIgnoreCase(line)) {
                    String listing = buildServiceListing();
                    out.write("OK|" + listing);
                    out.newLine();
                    out.flush();
                    continue;
                }

                if (line.startsWith("RUN|")) {
                    String resp = handleRun(line);
                    out.write(resp);
                    out.newLine();
                    out.flush();
                    continue;
                }

                out.write("ERROR|Unknown command. Use LIST or RUN|...");
                out.newLine();
                out.flush();
            }

        } catch (Exception e) {
            System.err.println("Client handler error (" + clientAddr + "): " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (Exception ignored) {}
            System.out.println("Client disconnected: " + clientAddr);
        }
    }

    private static String buildServiceListing() {
        // Return only alive nodes
        Map<String, List<NodeInfo>> byService = new HashMap<>();
        long now = System.currentTimeMillis();

        for (NodeInfo ni : nodesById.values()) {
            if ((now - ni.lastSeenMs) <= DEAD_AFTER_MS) {
                byService.computeIfAbsent(ni.service, k -> new ArrayList<>()).add(ni);
            }
        }

        if (byService.isEmpty()) return "No services available";

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<NodeInfo>> e : byService.entrySet()) {
            sb.append(e.getKey()).append(": ");
            List<NodeInfo> list = e.getValue();
            for (int i = 0; i < list.size(); i++) {
                NodeInfo ni = list.get(i);
                sb.append(ni.nodeId).append("@").append(ni.ip).append(":").append(ni.tcpPort);
                if (i < list.size() - 1) sb.append(", ");
            }
            sb.append(" ; ");
        }
        return sb.toString();
    }

    private static String handleRun(String line) {
        // RUN|SERVICE|OP|DATA
        String[] parts = line.split("\\|", 4);
        if (parts.length < 4) return "ERROR|Bad RUN format. Use RUN|SERVICE|OP|DATA";

        String service = parts[1].trim();
        String op = parts[2].trim();
        String data = parts[3]; // keep as-is (can contain spaces)

        NodeInfo target = pickAliveNodeForService(service);
        if (target == null) return "ERROR|No alive node found for service: " + service;

        // For your Base64 SN, the SN expects:
        // ENCODE|text
        // DECODE|base64text
        String snRequest = op.toUpperCase(Locale.ROOT) + "|" + data;

        try {
            String snResp = callServiceNode(target, snRequest);
            // Your SN returns either "ERROR|..." or the raw result string.
            if (snResp.startsWith("ERROR|")) return snResp;
            return "OK|" + snResp;

        } catch (Exception e) {
            System.err.println("SN call failed (" + target.nodeId + "): " + e.getMessage());
            return "ERROR|Service node failed: " + target.nodeId;
        }
    }

    private static NodeInfo pickAliveNodeForService(String service) {
        long now = System.currentTimeMillis();
        List<NodeInfo> alive = new ArrayList<>();

        for (NodeInfo ni : nodesById.values()) {
            if (service.equals(ni.service) && (now - ni.lastSeenMs) <= DEAD_AFTER_MS) {
                alive.add(ni);
            }
        }

        if (alive.isEmpty()) return null;

        // Round-robin selection
        rrCounters.putIfAbsent(service, new AtomicInteger(0));
        int idx = Math.floorMod(rrCounters.get(service).getAndIncrement(), alive.size());
        return alive.get(idx);
    }

    private static String callServiceNode(NodeInfo ni, String requestLine) throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(ni.ip, ni.tcpPort), SN_TCP_TIMEOUT_MS);
            s.setSoTimeout(SN_TCP_TIMEOUT_MS);

            try (
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8))
            ) {
                out.write(requestLine);
                out.newLine();
                out.flush();

                String resp = in.readLine();
                if (resp == null) throw new IOException("SN closed connection without response");
                return resp.trim();
            }
        }
    }

    // ================= DATA MODEL =================

    private static class NodeInfo {
        String nodeId;
        String service;  // e.g., "BASE64"
        String ip;       // source IP from UDP packet
        int tcpPort;     // advertised port in heartbeat
        long lastSeenMs;
    }
}