import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.Random;

/**
 * ImageService.java
 * by Andrew Goodman
 * 
 * This service handles image transformations like grayscale, resize, rotate,
 * and thumbnails.
 * 
 * How it works:
 * 1. Sends UDP heartbeats to the server every 15-30 seconds
 * 2. Listens for incoming TCP connections with image data
 * 3. Processes the image and sends the result back
 */
public class ImageService {

    // Service configuration
    static String NODE_ID = "ImageNode1";
    private static int MY_PORT;
    private static String SERVER_IP;
    private static int SERVER_UDP_PORT;

    static volatile boolean running = true;

    public static void main(String[] args) {

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
        MY_PORT = Integer.parseInt(config.getProperty("my.port"));
        SERVER_IP = config.getProperty("server.ip");
        SERVER_UDP_PORT = Integer.parseInt(config.getProperty("server.udp.port"));

        // Allow command line args to override defaults
        if (args.length >= 1)
            NODE_ID = args[0];
        if (args.length >= 2)
            MY_PORT = Integer.parseInt(args[1]);
        if (args.length >= 3)
            SERVER_IP = args[2];
        if (args.length >= 4)
            SERVER_UDP_PORT = Integer.parseInt(args[3]);

        System.out.println("=== IMAGE TRANSFORM SERVICE ===");
        System.out.println("Node ID: " + NODE_ID);
        System.out.println("Listening on port: " + MY_PORT);
        System.out.println("Server: " + SERVER_IP + ":" + SERVER_UDP_PORT);
        System.out.println("===============================\n");

        // Start heartbeat thread in the background
        new Thread(ImageService::sendHeartbeats).start();

        // Listen for incoming task requests
        listenForTasks();
    }

    // Sends periodic heartbeats to let the server know we're still running
    static void sendHeartbeats() {
        try {
            DatagramSocket socket = new DatagramSocket();
            InetAddress serverAddr = InetAddress.getByName(SERVER_IP);
            Random random = new Random();

            while (running) {
                // Build the heartbeat message
                String message = "HEARTBEAT|" + NODE_ID + "|IMAGE_TRANSFORM|" + MY_PORT;
                byte[] data = message.getBytes();

                // Send it to the server via UDP
                DatagramPacket packet = new DatagramPacket(data, data.length, serverAddr, SERVER_UDP_PORT);
                socket.send(packet);
                System.out.println("[Heartbeat] Sent: " + message);

                // Wait 15-30 seconds before sending the next one
                Thread.sleep(15000 + random.nextInt(15000));
            }
            socket.close();
        } catch (Exception e) {
            System.err.println("Heartbeat error: " + e.getMessage());
        }
    }

    // Listens for incoming TCP connections with tasks
    static void listenForTasks() {
        try {
            ServerSocket serverSocket = new ServerSocket(MY_PORT);
            System.out.println("[TCP] Listening for tasks on port " + MY_PORT + "...\n");

            while (running) {
                // Accept incoming connection
                Socket client = serverSocket.accept();
                System.out.println("[TCP] Received connection from " + client.getInetAddress());

                // Handle in separate thread so we can process multiple requests
                new Thread(() -> handleTask(client)).start();
            }
            serverSocket.close();
        } catch (Exception e) {
            System.err.println("TCP server error: " + e.getMessage());
        }
    }

    // Handles a single image processing request
    static void handleTask(Socket client) {
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());

            // Read the operation request
            String request = in.readUTF();
            String[] parts = request.split("\\|");
            String operation = parts[0];
            String params = parts.length > 1 ? parts[1] : "";
            System.out.println("[Task] Operation: " + operation + ", Params: " + params);

            // Read the image data
            long size = in.readLong();
            byte[] imageData = new byte[(int) size];
            in.readFully(imageData);
            System.out.println("[Task] Received " + size + " bytes of image data");

            // Process the image
            byte[] result = processImage(operation, params, imageData);

            // Send the result back
            if (result != null) {
                out.writeUTF("OK");
                out.writeLong(result.length);
                out.write(result);
                System.out.println("[Task] Sent " + result.length + " bytes back\n");
            } else {
                out.writeUTF("ERROR");
                out.writeLong(0);
            }
            out.flush();
            client.close();

        } catch (Exception e) {
            System.err.println("Task error: " + e.getMessage());
        }
    }

    // Applies the requested transformation to the image
    static byte[] processImage(String operation, String params, byte[] imageData) {
        try {
            // Convert bytes to BufferedImage
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageData));
            if (image == null) {
                System.err.println("ERROR: Could not read image (unsupported format or corrupted data)");
                return null;
            }
            System.out.println("[Task] Image loaded: " + image.getWidth() + "x" + image.getHeight());
            BufferedImage result;

            // Apply the requested operation
            switch (operation.toUpperCase()) {
                case "GRAYSCALE":
                    result = toGrayscale(image);
                    break;
                case "RESIZE":
                    result = resize(image, params); // params format: "800x600"
                    break;
                case "ROTATE":
                    result = rotate(image, Integer.parseInt(params)); // params: degrees (90, 180, 270)
                    break;
                case "THUMBNAIL":
                    result = thumbnail(image, Integer.parseInt(params)); // params: max size
                    break;
                default:
                    System.err.println("Unknown operation: " + operation);
                    return null;
            }

            // Convert result back to bytes
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(result, "png", baos);

            // Display before/after comparison window
            showBeforeAfter(image, result, operation);

            return baos.toByteArray();

        } catch (Exception e) {
            System.err.println("Image processing error: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    // Image Operations

    // Converts image to grayscale using luminosity formula
    static BufferedImage toGrayscale(BufferedImage img) {
        int w = img.getWidth(), h = img.getHeight();
        BufferedImage gray = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) {
                int rgb = img.getRGB(x, y);
                int r = (rgb >> 16) & 0xFF;
                int g = (rgb >> 8) & 0xFF;
                int b = rgb & 0xFF;
                int grayVal = (int) (0.299 * r + 0.587 * g + 0.114 * b); // Luminosity formula
                int grayRGB = (grayVal << 16) | (grayVal << 8) | grayVal;
                gray.setRGB(x, y, grayRGB);
            }
        }
        return gray;
    }

    // Resizes image to specified dimensions
    static BufferedImage resize(BufferedImage img, String params) {
        String[] dims = params.split("x");
        int newW = Integer.parseInt(dims[0]);
        int newH = Integer.parseInt(dims[1]);

        BufferedImage resized = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = resized.createGraphics();
        g.drawImage(img, 0, 0, newW, newH, null);
        g.dispose();
        return resized;
    }

    // Rotates image by 90, 180, or 270 degrees
    static BufferedImage rotate(BufferedImage img, int degrees) {
        int w = img.getWidth(), h = img.getHeight();
        int newW = (degrees == 90 || degrees == 270) ? h : w;
        int newH = (degrees == 90 || degrees == 270) ? w : h;

        BufferedImage rotated = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = rotated.createGraphics();
        g.translate((newW - w) / 2.0, (newH - h) / 2.0);
        g.rotate(Math.toRadians(degrees), w / 2.0, h / 2.0);
        g.drawImage(img, 0, 0, null);
        g.dispose();
        return rotated;
    }

    // Creates a smaller thumbnail while keeping aspect ratio
    static BufferedImage thumbnail(BufferedImage img, int maxSize) {
        int w = img.getWidth(), h = img.getHeight();
        double scale = Math.min((double) maxSize / w, (double) maxSize / h);
        int newW = (int) (w * scale);
        int newH = (int) (h * scale);
        return resize(img, newW + "x" + newH);
    }

    // Shows a popup window comparing the original and transformed images
    static void showBeforeAfter(BufferedImage before, BufferedImage after, String operation) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Image Transform: " + operation);
            frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

            // Scale images to fit in window
            Image scaledBefore = scaleToFit(before, 400);
            Image scaledAfter = scaleToFit(after, 400);

            JPanel panel = new JPanel(new GridLayout(1, 2, 10, 10));
            panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

            // Left panel shows original
            JPanel beforePanel = new JPanel(new BorderLayout());
            beforePanel.add(new JLabel("BEFORE", JLabel.CENTER), BorderLayout.NORTH);
            beforePanel.add(new JLabel(new ImageIcon(scaledBefore)), BorderLayout.CENTER);

            // Right panel shows result
            JPanel afterPanel = new JPanel(new BorderLayout());
            afterPanel.add(new JLabel("AFTER: " + operation, JLabel.CENTER), BorderLayout.NORTH);
            afterPanel.add(new JLabel(new ImageIcon(scaledAfter)), BorderLayout.CENTER);

            panel.add(beforePanel);
            panel.add(afterPanel);

            frame.add(panel);
            frame.pack();
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);
        });
    }

    // Scales image to fit within maxSize while keeping aspect ratio
    static Image scaleToFit(BufferedImage img, int maxSize) {
        int w = img.getWidth(), h = img.getHeight();
        if (w <= maxSize && h <= maxSize)
            return img;
        double scale = Math.min((double) maxSize / w, (double) maxSize / h);
        int newW = (int) (w * scale);
        int newH = (int) (h * scale);
        return img.getScaledInstance(newW, newH, Image.SCALE_SMOOTH);
    }
}
