package com.mashnet.node;

import com.mashnet.core.models.ComputationSchema;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.client.RsocketClientManager;
import com.mashnet.network.server.RsocketServerManager;
import com.mashnet.network.topology.TopologyManager;
import com.mashnet.stream.StreamManager;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;

import java.util.Map;
import java.util.Scanner;

/**
 * Console UI: Відповідає виключно за взаємодію з користувачем через консоль.
 * Використовує передані менеджери для виконання команд.
 */
public class ConsoleMenu {

    private final int myPort;
    private final TopologyManager topologyManager;
    private final RsocketServerManager serverManager;
    private final RsocketClientManager clientManager;
    private final StreamManager streamManager;

    private boolean isServerStarted = false;

    public ConsoleMenu(int myPort, TopologyManager topologyManager,
                       RsocketServerManager serverManager, RsocketClientManager clientManager,
                       StreamManager streamManager) {
        this.myPort = myPort;
        this.topologyManager = topologyManager;
        this.serverManager = serverManager;
        this.clientManager = clientManager;
        this.streamManager = streamManager;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        String command = "";
        String nodeId = topologyManager.getLocalNodeId();

        while (!command.equals("EXIT")) {
            System.out.println("\n--- MENU [" + nodeId + "] ---");
            System.out.println(" START  - Запустити сервер");
            System.out.println(" CREATE - Запустити ноду (Сервер + Клієнт)");
            System.out.println(" SEND   - Відправити LOAD_SCHEMA");
            System.out.println(" PORTS  - Доступні ноди");
            System.out.println(" SENSOR - Відкрити потік даних");
            System.out.println(" STOP   - Зупинити потоки");
            System.out.println(" RESET  - Скинути налаштування");
            System.out.println(" EXIT   - Вихід");
            System.out.print("Вибір: ");

            command = scanner.nextLine().toUpperCase();

            switch (command) {
                case "START" -> handleStart();
                case "CREATE" -> handleCreate(scanner);
                case "SEND" -> broadcastLoadSchema();
                case "PORTS" -> printPorts();
                case "SENSOR" -> streamManager.startAllSensorStreams();
                // Зверни увагу: метод stopAllStreams потрібно додати в StreamManager!
                case "STOP" -> System.out.println(">>> Зупинка потоків...");
                case "RESET" -> broadcastReset();
                case "EXIT" -> System.out.println("Завершення роботи ноди...");
                default -> {
                    if (!command.isEmpty()) System.out.println("Невідома команда: [" + command + "]");
                }
            }
        }
    }

    private void handleStart() {
        serverManager.startTcpServer(myPort);
        if (myPort == MeshNodeApp.SEED_PORT) {
            serverManager.startWebSocketServer(7001);
        } else {
            System.out.println("Підключення до SeedNode (" + MeshNodeApp.SEED_PORT + ")...");
            clientManager.connectToNeighbor(MeshNodeApp.SEED_PORT);
        }
        isServerStarted = true;
    }

    private void handleCreate(Scanner scanner) {
        if (!isServerStarted) {
            System.err.println("!!! Помилка: Спочатку натисніть START для запуску сервера.");
            return;
        }
        System.out.print("Порт для підключення: ");
        int p = scanner.nextInt();
        if (scanner.hasNextLine()) scanner.nextLine();
        clientManager.connectToNeighbor(p);
    }

    private void printPorts() {
        System.out.println("Активні з'єднання (Прямі сусіди): " + topologyManager.getActiveConnections().keySet());
        System.out.println("Топологія (Вся мережа): " + topologyManager.getTopologySnapshot().get("nodes"));
    }

    private void broadcastLoadSchema() {
        Map<String, RSocket> connections = topologyManager.getActiveConnections();
        if (connections.isEmpty()) {
            System.err.println("!!! Помилка: Немає активних підключень.");
            return;
        }

        System.out.println("Розсилка JSON LOAD_SCHEMA всім сусідам (" + connections.size() + ")...");
        ComputationSchema mySchema = new ComputationSchema("task-math-01", "MULTIPLY", "port:7001", "port:7002");

        try {
            String jsonSchema = JsonUtil.MAPPER.writeValueAsString(mySchema);
            connections.forEach((targetNodeId, rsocket) -> {
                Payload payload = DefaultPayload.create("LOAD_SCHEMA", jsonSchema);
                rsocket.requestResponse(payload).subscribe(
                        res -> System.out.println("<<< Відповідь від " + targetNodeId + ": " + res.getDataUtf8()),
                        err -> System.err.println("!!! Помилка: " + err.getMessage())
                );
            });
        } catch (Exception e) {
            System.err.println("!!! Помилка створення JSON: " + e.getMessage());
        }
    }

    private void broadcastReset() {
        Map<String, RSocket> connections = topologyManager.getActiveConnections();
        if (connections.isEmpty()) {
            System.err.println("!!! Помилка: Немає активних підключень.");
            return;
        }
        System.out.println("Розсилка команди RESET всім сусідам...");
        connections.forEach((targetNodeId, rsocket) -> rsocket.requestResponse(DefaultPayload.create("RESET"))
                .subscribe(
                        res -> System.out.println("<<< Відповідь від " + targetNodeId + ": " + res.getDataUtf8()),
                        err -> System.err.println("!!! Помилка: " + err.getMessage())
                ));
    }
}