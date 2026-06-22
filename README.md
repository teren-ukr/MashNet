# MashNet-Acoustic: High-Resilience Heterogeneous Mesh Network

![Status](https://img.shields.io/badge/Status-Prototype-orange)
![Protocol](https://img.shields.io/badge/Protocol-RSocket-blue)
![Security](https://img.shields.io/badge/Security-mTLS-success)

This repository contains a prototype of a distributed, point-to-point mesh network built with **RSocket**. Originally designed for high-speed heterogeneous computing, the current implementation is adapted for **real-time distributed acoustic monitoring** (e.g., UAV detection using TDOA and Cross-Correlation).

The system seamlessly integrates different programming environments (Java, Python, TypeScript) into a single reactive, resilient, and secure network.

## 🏗 System Architecture

The network consists of three main heterogeneous layers:

1. **Edge Sensors (Python):** Microcomputers equipped with microphones. They capture raw audio, pack it into chunks (to prevent TCP overhead), and stream it securely to the network.
2. **Compute & Seed Nodes (Java):** The core of the network built on Spring and Project Reactor. They act as RSocket routers, multiplex streams, and perform heavy mathematical computations (Cross-correlation, DSP).
3. **Control Dashboard (TypeScript / React):** A web-based UI that allows operators to visually construct data pipelines (using React Flow) and monitor acoustic telemetry in real-time (using HTML5 Canvas & Web Audio API).

## ✨ Key Features

* **Dynamic Visual Pipelines:** Operators can construct Directed Acyclic Graphs (DAGs) in the browser to route audio streams and deploy them to Java nodes "on the fly" without restarting servers.
* **Stream Multiplexing (Hot Streams):** Solves the network duplication problem. A single sensor stream can be shared across multiple UI visualizers and compute nodes simultaneously without overloading the Edge node.
* **Acoustic Signal Processing:** Built-in capabilities for real-time Cross-Correlation and GCC-PHAT to calculate the Time Difference of Arrival (TDOA) between distributed sensors.
* **Zero-Trust Security (mTLS):** All TCP and WebSocket connections are strictly encrypted. The system uses a standalone Local Root CA to ensure complete independence from the global Internet. Hardware UUIDs are bound to X.509 certificates.
* **High Resilience & Backpressure:** Utilizes RSocket's `.resume()` and `.reconnect()` features. The built-in backpressure mechanism dynamically prevents Java servers from Out-Of-Memory (OOM) errors if sensors transmit data too fast.

## 🛠 Network Interaction Model

The network utilizes the full power of the RSocket protocol, replacing standard REST APIs and WebSockets with a unified reactive transport:

* **Request-Response:** Used for configuration commands (e.g., `DEPLOY_SCHEMA`, `GET_TOPOLOGY`).
* **Fire-and-Forget:** Used for system-wide asynchronous events that don't require confirmation (e.g., `STOP_ALL`, `NEW_EDGE`).
* **Request-Stream:** Used for continuous, low-latency transmission of acoustic telemetry and calculated arrays.

## 🎛 Control Plane Commands

The distributed nodes are managed via a dedicated control channel:
* `DEPLOY_SCHEMA` - Transmits a compiled JSON configuration to construct a reactive pipeline on a target node.
* `SUBSCRIBE_STREAM` - Requests a routed data flow from a specific sensor or compute block.
* `STOP` / `RESET` - Gracefully disposes of active Reactor chains and frees memory on a specific node without dropping the physical SSL connection.
* `STOP_ALL` - A global emergency stop that completely clears all active processing pipelines across the mesh.

## 🎓 Academic Context
This repository is developed as a practical implementation for the diploma project: *"Mesh Network for Heterogeneous High-Speed Computing"*.
