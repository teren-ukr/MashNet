package com.mashnet.node;

import com.mashnet.network.client.RsocketClientManager;
import com.mashnet.network.server.RsocketServerManager;
import com.mashnet.network.topology.TopologyManager;
import com.mashnet.stream.StreamManager;

import java.util.Scanner;
import java.util.UUID;

/**
 * Точка входу.
 * Виконує роль "Composition Root" (Збирає всі залежності програми разом).
 */
public class MeshNodeApp {

    public static final int SEED_PORT = 7000;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Мій порт (Сервер): ");
        int myPort = scanner.nextInt();

        String nodeId = (myPort == SEED_PORT)
                ? "Seed-" + UUID.randomUUID().toString().substring(0, 4)
                : "JavaNode-" + UUID.randomUUID().toString().substring(0, 4);

        System.out.println("=========================================");
        System.out.println(" Ініціалізація ноди: " + nodeId);
        System.out.println("=========================================");

        // 1. Dependency Injection (Впровадження залежностей)
        //паттерн Composition Root
        TopologyManager topologyManager = new TopologyManager(nodeId);
        StreamManager streamManager = new StreamManager(topologyManager);
        RsocketClientManager clientManager = new RsocketClientManager(topologyManager, myPort, streamManager);
        RsocketServerManager serverManager = new RsocketServerManager(topologyManager, clientManager, streamManager);



        // 2. Передача керування в UI-шар
        ConsoleMenu menu = new ConsoleMenu(myPort, topologyManager, serverManager, clientManager, streamManager);
        menu.start();
    }
}