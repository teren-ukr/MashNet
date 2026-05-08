package com.mashnet.network.topology;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
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

    // реєстр активних потоків ( dataSource -> dataTarget )
    private final Set<String> activeStreamEdges = ConcurrentHashMap.newKeySet();

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
     * @return Map зі списками "nodes", "edges" та "activeStreams".
     */
    public Map<String, Object> getTopologySnapshot(){
        // 1. Збираємо всіх прямих сусідів і себе
        List<String> nodeList = new ArrayList<>(activeConections.keySet());
        nodeList.add(localNodeId);

        // 2. Збираємо всі чутки про інші ноди (Gossip)
        List<Map<String, String>> allEdges = new ArrayList<>(globalEdges);

        // 3. Додаємо прямі зв'язки, яких нема в чутках
        for(String connectedNode : activeConections.keySet()){
            Map<String, String> edge = new HashMap<>();
            edge.put("source", connectedNode);
            edge.put("target", localNodeId);

            if(!allEdges.contains(edge)){
                allEdges.add(edge);
            }
        } // <--- ЦИКЛ ЗАКІНЧУЄТЬСЯ ТУТ. Ми перебрали ВСІХ сусідів!

        // 4. Пакуємо об'єкт для подальшої конвертації в JSON
        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("nodes", nodeList);
        snapshot.put("edges", allEdges);
        snapshot.put("activeStreams", new ArrayList<>(activeStreamEdges));

        return snapshot; // Повертаємо повноцінний зліпок
    }

    /**
     * Відправляє системну подію про зміну топології у шину (Event Bus).
     */
    private  void broadcastTopologyChange(){
        eventSink.tryEmitNext("{\"event\": \"TOPOLOGY_CHANGED\"}");
    }

    /**
     * Оновлює статус потоку і запускає ланцюгову реакцію (Gossip) по мережі.
     * Повертає true, якщо стан дійсно змінився (захист від нескінченних циклів).
     */
    public boolean setStreamStatusAndBroadcast(String dataSource, String dataTarget, boolean isActive) {
        String edgeId = dataSource + "->" + dataTarget;
        boolean changed;

        if (isActive) {
            changed = activeStreamEdges.add(edgeId);
        } else {
            changed = activeStreamEdges.remove(edgeId);
        }

        if (changed) {
            // Сповіщаємо локальний UI та інші модулі (викличе TOPOLOGY_CHANGED)
            broadcastTopologyChange();

            // Відправляємо Gossip сусідам
            String json = "{\"dataSource\":\"" + dataSource + "\", \"dataTarget\":\"" + dataTarget + "\", \"isActive\":" + isActive + "}";
            for (Map.Entry<String, io.rsocket.RSocket> entry : activeConections.entrySet()) {
                String neighborId = entry.getKey();
                if (!neighborId.contains("Dashboard") && !neighborId.contains("UI")) {
                    io.rsocket.Payload payload = io.rsocket.util.DefaultPayload.create("STREAM_EVENT", json);
                    entry.getValue().fireAndForget(payload).subscribe();
                }
            }
        }
        return changed;
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
