package com.mashnet.network.topology;

import io.rsocket.RSocket;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Клас TopologyManager відповідає за зберігання стану мережі (Gossip Discovery).
 * Він не знає про те, як передаються дані, його єдина мета — знати, хто до кого підключений.
 */
public class TopologyManager {

    private final String localNodeId;

    //реєстр активних сокетів(серверів датчиків тд)
    private final Map<String, RSocket> activeConections = new ConcurrentHashMap<>();

    //глобальний список зв'язків (source -> target)
    private final List<Map<String, String>> globalEdges = new CopyOnWriteArrayList<>();

    // реєстр активних потоків
    private final Set<String> activeStreamNodes = ConcurrentHashMap.newKeySet();

    //шина подій
    private final Sinks.Many<String> eventSink = Sinks.many().multicast().directBestEffort();


    /**
     * Конструктор менеджера топології.
     *
     * @param localNodeId унікальний ID поточної ноди.
     */
    public TopologyManager(String localNodeId) {
        this.localNodeId = localNodeId;
    }


    /**
     * Додає нове пряме підключення до реєстру.
     *
     * @param remoteNodeId ID сусідньої ноди.
     * @param socket       RSocket з'єднання з цією нодою.
     */
    public void addConnection(String remoteNodeId, RSocket socket) {
        activeConections.put(remoteNodeId, socket);
        System.out.println(">>> [TOPOLOGY] Node registered: " + remoteNodeId);
        broadcastTopologyChange();
    }


    /**
     * Видаляє ноду з мережі (Garbage Collector для графа).
     * Викликається автоматично, коли розривається TCP або WebSocket з'єднання.
     *
     * @param disconnectedNodeId ID ноди, яка відключилася.
     */
    public void handleDisconnection(String disconnectedNodeId) {
        System.out.println(">>> [TOPOLOGY] Cleaning up node traces: " + disconnectedNodeId);

        //видаляємо з активних підключень
        activeConections.remove(disconnectedNodeId);

        //видаляємо gossips(ребра де нода бюуло ціллю або джерелом) про цю ноду, щоб про неї всі забули
        boolean adgesRemoved = globalEdges
                .removeIf(
                        edge -> disconnectedNodeId.equals(edge.get("source")) ||
                                disconnectedNodeId.equals(edge.get("target"))
                );

        System.out.println(">>> [TOPOLOGY] Cleaning completed");

        //даємо пінок всім модулям, що відбулися зміни
        broadcastTopologyChange();
    }


    /**
     * Реєструє новий зв'язок десь у мережі (на основі пліток Gossip Protocol).
     *
     * @param source ID ноди-джерела.
     * @param target ID ноди-цілі.
     */
    public void addGlobalEdge(String source, String target){
        Map<String, String> edge = new HashMap<>();
        edge.put("source", source);
        edge.put("target", target);

        //якщо такої нема, то додаємо до списку
        if(!globalEdges.contains(edge)){
            globalEdges.add(edge);
            System.out.println("[MESH-DISCOVERY] New node: " + source + " -> " + target);
            broadcastTopologyChange();
        }
    }

    /**
     * Генерує повну карту мережі для відправки на Дашборд.
     * Об'єднує прямі підключення та глобальні плітки (Mesh).
     *
     * @return Map зі списками "nodes" (вузли) та "edges" (зв'язки).
     */
    public Map<String, Object> getTopologySnapshot(){
        //збираємо всіх прямих сусідів і себе
        List<String> nodeList = new ArrayList<>(activeConections.keySet());
        nodeList.add(localNodeId);

        //збираємо всі чутки про інші ноди
        List<Map<String, String>> allEdges = new ArrayList<>(globalEdges);

        //додаємо прямі зв'язки, яких нема в чутках
        for(String connectedNode : activeConections.keySet()){
            Map<String, String> edge = new HashMap<>();
            edge.put("source", connectedNode);
            edge.put("target", localNodeId);

            if(!allEdges.contains(edge)){
                allEdges.add(edge);
            }

            //пакуємо об'єкт для подальшої конвертації в json
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("nodes", nodeList);
            snapshot.put("edges", allEdges);
            snapshot.put("activeStreams",new ArrayList<>(activeStreamNodes));
            return snapshot;
        }

        return null;
    }

    /**
     * Відправляє системну подію про зміну топології у шину (Event Bus).
     */
    private  void broadcastTopologyChange(){
        eventSink.tryEmitNext("{\"event\": \"TOPOLOGY_CHANGED\"}");
    }

    /**
     * Змінює значення чи активна нода.
     * @param nodeId
     * @param isActivity
     */
    public void setStreamStatus(String nodeId, boolean isActive){
        if (isActive) {
            activeStreamNodes.add(nodeId);
        } else {
            activeStreamNodes.remove(nodeId);
        }
        broadcastTopologyChange(); // Повідомляємо всіх про зміну стану
    }


    // ---- GETTERS ---- ///

    public Sinks.Many<String> getEventSink() {
        return eventSink;
    }

    public Map<String, RSocket> getActiveConnections() {
        return activeConections;
    }

    public String getLocalNodeId() {
        return localNodeId;
    }
}
