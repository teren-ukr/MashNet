package com.mashnet.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.topology.TopologyManager;
import com.mashnet.stream.sink.DataAggregatorSink;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data Stream Layer: Керує життєвим циклом потоків даних.
 * Відкриває та закриває труби (Streams) до датчиків.
 */
public class StreamManager {

    private final TopologyManager topologyManager;
    private final DataAggregatorSink dataAggregator; // Наш новий математичний модуль

    // Реєстр активних "пультів керування" (Disposable) для кожного потоку
    private final Map<String, Disposable> activeStreams = new ConcurrentHashMap<>();

    public StreamManager(TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        this.dataAggregator = new DataAggregatorSink();

        // Підписуємося на події мережі (щоб закривати потоки "мертвих" нод)
        listenToNetworkEvents();
    }

    /**
     * Слухач системних подій (Паттерн Observer).
     */
    private void listenToNetworkEvents() {
        topologyManager.getEventSink().asFlux().subscribe(jsonEvent -> {
            try {
                JsonNode eventNode = JsonUtil.MAPPER.readTree(jsonEvent);
                if ("NODE_DISCONNECTED".equals(eventNode.get("event").asText())) {
                    String disconnectedNodeId = eventNode.get("nodeId").asText();
                    stopStreamForNode(disconnectedNodeId);
                }
            } catch (Exception e) {
                // Ігноруємо помилки парсингу системних подій
            }
        });
    }

    /**
     * Відправляє команду START_SENSOR всім сусідам і відкриває потік даних.
     */
    public void startAllSensorStreams() {
        Map<String, RSocket> connections = topologyManager.getActiveConnections();

        if (connections.isEmpty()) {
            System.err.println("!!! Помилка: Немає активних підключень для збору даних.");
            return;
        }

        System.out.println(">>> [STREAM] Запит потоку START_SENSOR від усіх підключених нод...");

        connections.forEach((targetNodeId, rsocket) -> {
            // Пропускаємо Дашборди
            if (targetNodeId.contains("Dashboard") || targetNodeId.contains("UI")) return;

            // Якщо потік вже відкрито — не відкриваємо двічі
            if (activeStreams.containsKey(targetNodeId) && !activeStreams.get(targetNodeId).isDisposed()) return;

            // 1. Сповіщаємо UI (зелена лінія)
            topologyManager.getEventSink().tryEmitNext("{\"event\": \"STREAM_START\", \"nodeId\": \"" + targetNodeId + "\"}");

            // 2. Відкриваємо сирий потік і парсимо байти в Double
            var rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                    .map(payload -> Double.parseDouble(payload.getDataUtf8()));

            // 3. Передаємо сирий потік у наш Sink (Приймач) для математики
            Disposable streamDisposable = dataAggregator.processStream(rawStream, targetNodeId)
                    .doOnError(e -> System.err.println(">>> [STREAM] Зв'язок з нодою " + targetNodeId + " втрачено."))
                    .onErrorResume(e -> Mono.empty())
                    .doFinally(signalType -> {
                        // 4. Сповіщаємо UI про зупинку (сіра лінія)
                        topologyManager.getEventSink().tryEmitNext("{\"event\": \"STREAM_STOP\", \"nodeId\": \"" + targetNodeId + "\"}");

                        if (signalType == SignalType.CANCEL) {
                            System.out.println(">>> [STREAM] Потік призупинено (Нода " + targetNodeId + " залишається на зв'язку).");
                        } else {
                            System.out.println(">>> [STREAM] Потік завершився для ноди " + targetNodeId);
                        }
                    })
                    .subscribe(); // Активуємо (відкриваємо вентиль)

            // Зберігаємо пульт керування потоком
            activeStreams.put(targetNodeId, streamDisposable);
        });
    }

    /**
     * Примусово зупиняє всі активні потоки (Команда STOP).
     */
    public void stopAllStreams() {
        if (activeStreams.isEmpty()) {
            System.out.println(">>> [STREAM] Немає активних потоків для зупинки.");
            return;
        }

        activeStreams.forEach((targetNodeId, streamDisposable) -> {
            if (!streamDisposable.isDisposed()) {
                streamDisposable.dispose(); // Надсилає сигнал CANCEL на датчик
                System.out.println(">>> [STREAM] Потік з нодою " + targetNodeId + " зупинено.");
            }
        });

        activeStreams.clear();
    }

    /**
     * Зупиняє потік тільки для однієї конкретної ноди (використовується при обриві зв'язку).
     */
    private void stopStreamForNode(String nodeId) {
        Disposable stream = activeStreams.remove(nodeId);
        if (stream != null && !stream.isDisposed()) {
            stream.dispose();
            System.out.println(">>> [STREAM] GC: Потік для мертвої ноди " + nodeId + " очищено.");
        }
    }
}