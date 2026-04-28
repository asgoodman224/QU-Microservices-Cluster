# QU Microservices Cluster

A Java-based distributed microservices cluster built from scratch. A central **Cluster Server** acts as a registry and proxy: service nodes self-register via UDP heartbeats, and clients connect over TCP to discover and invoke services through a unified binary protocol. The server performs round-robin load balancing across all live nodes for a given service and pushes real-time topology updates to subscribed clients.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Cluster Server                    │
│                   (ServerMain)                      │
│                                                     │
│  ┌─────────────────┐   ┌──────────────────────────┐ │
│  │  UDP Heartbeat  │   │   TCP Client Listener    │ │
│  │  Listener       │   │   (binary + text)        │ │
│  └────────┬────────┘   └────────────┬─────────────┘ │
│           │                         │               │
│  Node Registry (ConcurrentHashMap)  │               │
│  Dead Node Reaper (every 5s)        │               │
│  Round-Robin Load Balancer          │               │
└───────────┼─────────────────────────┼───────────────┘
            │ UDP heartbeats          │ TCP (persistent)
            │                         │
   ┌────────┴──────┐         ┌────────┴──────────┐
   │ Service Nodes │         │    Clients        │
   │               │         │  (TestClient2)    │
   │ - BASE64      │         │                   │
   │ - CSV_STATS   │         │  - Interactive    │
   │ - compression │         │  - One-shot       │
   │ - hmac_verify │         └───────────────────┘
   │ - IMAGE_TRANS │
   └───────────────┘
```

**How it works:**
1. Each service node starts, loads `config.properties`, and begins sending **UDP heartbeats** to the cluster server every 15–30 seconds.
2. The cluster server receives heartbeats, registers nodes, and reaps any node it has not heard from in 2 minutes.
3. When a client connects over **TCP**, it can request the live service list (`LIST`) or invoke a service (`RUN|<SERVICE>|<OP>|<PARAMS>`).
4. The server picks a live node using **round-robin** and forwards the request over a direct TCP connection to that node.
5. Clients that send `SUBSCRIBE` receive **push notifications** whenever a node joins or leaves the cluster.

---

## Services

| Service Name | Class | Operations |
|---|---|---|
| `BASE64` | `Base64ServiceNode` | `ENCODE`, `DECODE` |
| `CSV_STATS` | `CSVService` | `MEAN`, `MEDIAN`, `MIN`, `MAX`, `STDDEV`, `ALL` |
| `compression` | `CompressionServiceNode` | `COMPRESS`, `DECOMPRESS` |
| `hmac_verify` | `HmacServiceNode` | `SIGN`, `VERIFY` |
| `IMAGE_TRANSFORM` | `ImageService` | `GRAYSCALE`, `RESIZE`, `ROTATE`, `THUMBNAIL` |

### BASE64
Encodes raw bytes to Base64 text, or decodes Base64 text back to raw bytes. Works on any file type.

### CSV Statistics
Parses a CSV file and computes column-level statistics. `ALL` returns mean, median, min, max, and standard deviation for every numeric column.

### Compression
GZIP compresses or decompresses arbitrary file bytes. Works on any file type.

### HMAC-SHA256
Signs file bytes with a shared secret key (produces a hex HMAC-SHA256 signature), or verifies a file against a provided signature.

### Image Transform
Applies transformations to PNG/JPEG images:
- **GRAYSCALE** — converts to grayscale using the luminosity formula
- **RESIZE** — resizes to `WxH` (e.g. `400x300`)
- **ROTATE** — rotates by `90`, `180`, or `270` degrees
- **THUMBNAIL** — scales to fit within a maximum pixel dimension while keeping aspect ratio

---

## Prerequisites

- **Java 11+** (all code uses only the standard library)
- No external dependencies or build tools required

---

## Configuration

Every component reads a **`config.properties`** file from its working directory. Create one for each component you run.

### Cluster Server (`ServerMain`)

```properties
server.tcp.port=5100
server.udp.port=5200
```

### Service Nodes (`CSVService`, `ImageService`)

```properties
my.port=5101
server.ip=127.0.0.1
server.udp.port=5200
```

### Service Nodes (`Base64ServiceNode`)

```properties
sn.tcp.port=5102
server.ip=127.0.0.1
server.udp.port=5200
```

### Service Nodes (`CompressionServiceNode`, `HmacServiceNode`)

```properties
tcp.port=5103
server.host=127.0.0.1
server.port=5200
```

> For `HmacServiceNode`, also add:
> ```properties
> secret=your_shared_secret_key
> ```

---

## Compiling

Compile everything from the project root:

```bash
# Compile top-level service files
javac ServerMain.java Base64ServiceNode.java CSVService.java \
      CompressionServiceNode.java HmacServiceNode.java ImageService.java TestClient.java

# Compile the full test client (inside src/)
javac -d src/ src/cluster/TestClient2.java
```

---

## Running

Start each component in its own terminal. Each must have a `config.properties` in the directory you run from.

**1. Start the cluster server:**
```bash
java ServerMain
```

**2. Start one or more service nodes:**
```bash
java CSVService
java ImageService
java Base64ServiceNode
java CompressionServiceNode
java HmacServiceNode
```

**3. Run the client:**
```bash
# From the src/ directory (pre-compiled jar is included)
java -cp src/cluster/cluster.jar cluster.TestClient2 interactive 127.0.0.1 5100
```

Or use one-shot mode (no interactive shell):
```bash
java -cp src/cluster/cluster.jar cluster.TestClient2 list 127.0.0.1 5100
java -cp src/cluster/cluster.jar cluster.TestClient2 csv 127.0.0.1 5100 all data.csv
java -cp src/cluster/cluster.jar cluster.TestClient2 image 127.0.0.1 5100 grayscale photo.png
java -cp src/cluster/cluster.jar cluster.TestClient2 image 127.0.0.1 5100 resize photo.png 400x300
java -cp src/cluster/cluster.jar cluster.TestClient2 base64 127.0.0.1 5100 encode photo.png
java -cp src/cluster/cluster.jar cluster.TestClient2 compression 127.0.0.1 5100 compress document.pdf
java -cp src/cluster/cluster.jar cluster.TestClient2 hmac 127.0.0.1 5100 sign photo.png
java -cp src/cluster/cluster.jar cluster.TestClient2 hmac 127.0.0.1 5100 verify photo.png <signature>
```

> **Tip:** For `base64`, `compression`, and `hmac`, if the last argument is an existing file path its bytes are sent; otherwise the argument is treated as a literal text string.

---

## Client Modes

### Interactive Mode

Maintains a persistent connection to the server and receives **live push updates** whenever the cluster topology changes (node joins or leaves).

```
java -cp src/cluster/cluster.jar cluster.TestClient2 interactive 127.0.0.1 5100

=== INTERACTIVE MODE ===
Commands:
  list
  csv <all|mean|median|min|max|stddev> <file.csv>
  image <grayscale|resize|rotate|thumbnail> <file> [params]
  base64 <encode|decode> <file_or_text>
  compression <compress|decompress> <file_or_text>
  hmac <sign|verify> <file_or_text> [signature]
  quit
```

### One-Shot Mode

Opens a connection, sends a single command, prints the result, and exits. Useful for scripting.

---

## Direct Node Testing (TestClient)

`TestClient.java` connects directly to a service node, bypassing the cluster server. Useful for testing a single node in isolation.

```bash
java TestClient image grayscale testdata/test_image.png
java TestClient image resize  testdata/test_image.png 400x300
java TestClient csv   all     testdata/weather.csv
java TestClient csv   mean    testdata/weather.csv
```

Default ports: Image = `5101`, CSV = `5102`. Edit the constants in `TestClient.java` to match your config.

---

## Binary Protocol

All communication between the cluster server and service nodes uses the same binary framing over TCP:

**Request (client → node):**
```
writeUTF("OP|params")
writeLong(dataLength)
write(data)
```

**Response (node → client):**
```
readUTF("OK" or "ERROR")
readLong(resultLength)
readFully(result)
```

The cluster server wraps this with an envelope for client connections:

**Server → client response:**
```
writeUTF("RESPONSE")
writeUTF("OK" or "ERROR")
writeLong(length)
write(data)
```

**Server → client push (unsolicited):**
```
writeUTF("PUSH")
writeUTF("SERVICE_LIST")
writeLong(length)
write(data)
```

---

## Test Data

Sample files are included in `testdata/`:
- `test_image.png` — colour PNG for image transform tests
- `test_image_grayscale.png` — expected grayscale output for reference
- `weather.csv` — sample CSV for statistics tests

---

## Project Structure

```
.
├── ServerMain.java              # Cluster server (heartbeat listener, client TCP, load balancer)
├── Base64ServiceNode.java       # BASE64 encode/decode service node
├── CSVService.java              # CSV statistics service node
├── CompressionServiceNode.java  # GZIP compression service node
├── HmacServiceNode.java         # HMAC-SHA256 sign/verify service node
├── ImageService.java            # Image transform service node
├── TestClient.java              # Simple direct-to-node test client
├── testdata/
│   ├── test_image.png
│   ├── test_image_grayscale.png
│   └── weather.csv
└── src/cluster/
    ├── TestClient2.java         # Full cluster test client (interactive + one-shot)
    └── cluster.jar              # Pre-compiled jar of TestClient2
```
