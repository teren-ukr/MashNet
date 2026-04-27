# High-Resilience Heterogeneous Mesh Network (Prototype)

This repository contains a prototype of a distributed, point-to-point mesh network built with **RSocket**. The system is designed to perform high-speed computations and data streaming across different programming languages with high resilience.

##  Current Status: Core Backend Completed
The current implementation successfully meets the core architectural requirements of the technical specification. 

### Key Features Implemented:
* **Dual Node Role (Client/Server):** Every node in the network acts as both a server and a client simultaneously.
* **Security (X.509):** All connections, including local ones, are secured using TLS/SSL with certificates.
* **Dynamic Topology:** Nodes receive routing instructions at runtime using JSON schemas (`LOAD_SCHEMA`), allowing for dynamic reconfiguration and closed-loop data flows.
* **High Resilience:** The network uses RSocket's `.resume()` and `.reconnect()` features. It can survive network drops and automatically handles node disconnections without crashing.
* **Data Flow Control (Backpressure):** The system prevents memory overload. The Java node processes incoming data streams from Python sensors in real-time (calculating averages per second) using Project Reactor.

### 🛠 Control Channel Commands
The network uses a dedicated control channel to manage distributed nodes:
1. `LOAD_SCHEMA` - Sends a JSON configuration to nodes.
2. `SENSOR (START)` - Opens a `Request-Stream` channel to start high-speed data transmission.
3. `STOP` - Gracefully closes the data stream (`cancel` / `dispose`) while keeping the physical SSL connection alive.
4. `RESET` - Clears the current schema and stops tasks, returning the node to a standby state.

###  Technologies Used
* **Java:** Spring/Project Reactor (Seed Node / Compute Node)
* **Python:** `asyncio` & `rsocket-py` (Edge Node / Sensor Generator)
* **Protocol:** TCP + SSL + RSocket
