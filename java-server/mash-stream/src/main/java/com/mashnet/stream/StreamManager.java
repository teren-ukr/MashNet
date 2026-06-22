package com.mashnet.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.mashnet.core.models.ComputationSchema;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.control.IStreamProvider;
import com.mashnet.network.topology.TopologyManager;
import com.mashnet.stream.elements.MeshElement;
import com.mashnet.stream.elements.MeshElementFactory;
import com.mashnet.stream.sink.DataAggregatorSink;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data Stream Layer: Керує життєвим циклом потоків даних.
 * Відкриває та закриває труби (Streams) до датчиків.
 * Імплементує IStreamProvider для роздачі потоків іншим нодам.
 */
public class StreamManager implements IStreamProvider {

    private final TopologyManager topologyManager;
    private final DataAggregatorSink dataAggregator;
    private final Map<String, Disposable> activeStreams = new ConcurrentHashMap<>();

    // Реєстр "гарячих" потоків. Тепер Flux<String> для передачі масивів (JSON)
    private final Map<String, Flux<String>> processedOutputs = new ConcurrentHashMap<>();

    public StreamManager(TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        this.dataAggregator = new DataAggregatorSink();
        listenToNetworkEvents();
    }

    private void listenToNetworkEvents() {
        topologyManager.getEventSink().asFlux().subscribe(jsonEvent -> {
            try {
                JsonNode eventNode = JsonUtil.MAPPER.readTree(jsonEvent);
                if ("NODE_DISCONNECTED".equals(eventNode.get("event").asText())) {
                    String disconnectedNodeId = eventNode.get("nodeId").asText();
                    stopStreamForNode(disconnectedNodeId);
                }
            } catch (Exception e) {
                // Ігноруємо помилки парсингу
            }
        });
    }

    public void startAllSensorStreams() {
        Map<String, RSocket> connections = topologyManager.getActiveConnections();

        if (connections.isEmpty()) {
            System.err.println("!!! Помилка: Немає активних підключень для збору даних.");
            return;
        }

        System.out.println(">>> [STREAM] Запит потоку START_SENSOR від усіх підключених нод...");

        connections.forEach((targetNodeId, rsocket) -> {
            if (targetNodeId.contains("Dashboard") || targetNodeId.contains("UI")) return;
            if (activeStreams.containsKey(targetNodeId) && !activeStreams.get(targetNodeId).isDisposed()) return;

            ComputationSchema currentSchema = topologyManager.getCurrentSchema();
            String myNodeId = topologyManager.getLocalNodeId();
            topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, true);

            System.out.println(">>> [STREAM] Відкриваємо канал Request-Stream до " + targetNodeId);

            // Отримуємо рядок (JSON) замість парсингу Double + додаємо publish().refCount()
            Flux<String> rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                    .map(Payload::getDataUtf8)
                    .doOnError(e -> System.err.println(">>> [STREAM] Помилка потоку від " + targetNodeId + ": " + e.getMessage()))
                    .publish()
                    .refCount(1, java.time.Duration.ofSeconds(2));

            Map<String, Flux<String>> inputMap = Map.of("default-input", rawStream);
            Flux<String> sharedStream = buildPipeline(inputMap, currentSchema);

            processedOutputs.put(targetNodeId, sharedStream);

            Disposable streamDisposable = sharedStream
                    .doOnError(e -> System.err.println(">>> [STREAM] Зв'язок з нодою " + targetNodeId + " втрачено."))
                    .onErrorResume(e -> Mono.empty())
                    .doFinally(signalType -> {
                        topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, false);
                        processedOutputs.remove(targetNodeId);
                        if (signalType == SignalType.CANCEL) {
                            System.out.println(">>> [STREAM] Потік призупинено (Нода " + targetNodeId + " залишається на зв'язку).");
                        } else {
                            System.out.println(">>> [STREAM] Потік завершився для ноди " + targetNodeId);
                        }
                    })
                    .subscribe();

            activeStreams.put(targetNodeId, streamDisposable);
        });
    }

    @Override
    public void startStreamFrom(String targetNodeId) {
        Map<String, RSocket> connections = topologyManager.getActiveConnections();
        RSocket rsocket = connections.get(targetNodeId);

        if (rsocket == null) {
            System.err.println("!!! Помилка: Вузол " + targetNodeId + " не знайдено для запуску потоку.");
            return;
        }

        if (activeStreams.containsKey(targetNodeId) && !activeStreams.get(targetNodeId).isDisposed()) {
            System.out.println(">>> [STREAM] Потік з " + targetNodeId + " вже активний.");
            return;
        }

        String myNodeId = topologyManager.getLocalNodeId();
        topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, true);

        System.out.println(">>> [STREAM] Відкриваємо канал Request-Stream до " + targetNodeId);

        // Отримуємо рядок (JSON) замість парсингу Double + додаємо publish().refCount()
        Flux<String> rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                .map(Payload::getDataUtf8)
                .doOnError(e -> System.err.println(">>> [STREAM] Помилка потоку від " + targetNodeId + ": " + e.getMessage()))
                .publish()
                .refCount(1, java.time.Duration.ofSeconds(2));

        ComputationSchema currentSchema = topologyManager.getCurrentSchema();
        Map<String, Flux<String>> inputMap = Map.of("default-input", rawStream);
        Flux<String> sharedStream = buildPipeline(inputMap, currentSchema);

        processedOutputs.put(targetNodeId, sharedStream);

        Disposable streamDisposable = sharedStream
                .doOnError(e -> System.err.println(">>> [STREAM] Зв'язок з нодою " + targetNodeId + " втрачено."))
                .onErrorResume(e -> Mono.empty())
                .doFinally(signalType -> {
                    topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, false);
                    processedOutputs.remove(targetNodeId);
                    if (signalType == SignalType.CANCEL) {
                        System.out.println(">>> [STREAM] Потік призупинено (Нода " + targetNodeId + " залишається на зв'язку).");
                    } else {
                        System.out.println(">>> [STREAM] Потік завершився для ноди " + targetNodeId);
                    }
                })
                .subscribe();

        activeStreams.put(targetNodeId, streamDisposable);
    }

    public void startPipeline() {
        ComputationSchema schema = topologyManager.getCurrentSchema();
        if (schema == null || schema.inputSources == null || schema.inputSources.isEmpty()) {
            System.err.println(">>> [STREAM] Схема не містить джерел даних.");
            return;
        }

        Map<String, Flux<String>> sourceStreams = new HashMap<>();
        String myNodeId = topologyManager.getLocalNodeId();

        for (Map.Entry<String, String> entry : schema.inputSources.entrySet()) {
            String portId = entry.getKey();
            String sourceValue = entry.getValue();

            String targetNodeId;
            String requestPayload;

            if (sourceValue.startsWith("STREAM:")) {
                String[] parts = sourceValue.split(":");
                targetNodeId = parts[1];
                requestPayload = "SUBSCRIBE_STREAM:" + parts[2];
            } else {
                targetNodeId = sourceValue;
                requestPayload = "START_SENSOR";
            }

            // МАРШРУТИЗАЦІЯ (ПРОКСІЮВАННЯ)
            RSocket rsocket = topologyManager.getActiveConnections().get(targetNodeId);
            String routedPayload = requestPayload;

            if (rsocket == null) {
                // Якщо прямого з'єднання немає, просимо Seed-ноду передати запит
                for (Map.Entry<String, RSocket> activeEntry : topologyManager.getActiveConnections().entrySet()) {
                    if (activeEntry.getKey().startsWith("Seed-")) {
                        rsocket = activeEntry.getValue();
                        // Пакуємо запит для проксі-сервера (Seed-ноди)
                        routedPayload = "SUBSCRIBE_STREAM:" + targetNodeId + ":" + requestPayload;
                        System.out.println(">>> [STREAM] Маршрутизація запиту до " + targetNodeId + " через " + activeEntry.getKey());
                        break;
                    }
                }
            }

            if (rsocket != null) {
                topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, true);

                Flux<String> rawStream = rsocket.requestStream(DefaultPayload.create(routedPayload))
                        .map(Payload::getDataUtf8)
                        .doOnError(e -> System.err.println(">>> [STREAM] Помилка потоку від " + targetNodeId + ": " + e.getMessage()))
                        .publish()
                        .refCount(1, java.time.Duration.ofSeconds(2));

                sourceStreams.put(portId, rawStream);
                System.out.println(">>> [STREAM] Порт [" + portId + "] підключено. Відправлено запит: " + routedPayload);
            } else {
                System.err.println(">>> [STREAM] УВАГА: Вузол " + targetNodeId + " недоступний у мережі!");
            }

        }

        if (sourceStreams.isEmpty()) return;

        Flux<String> finalStream = buildPipeline(sourceStreams, schema);

        processedOutputs.put(schema.schemaId, finalStream);

        Disposable streamDisposable = finalStream
                .onErrorResume(e -> reactor.core.publisher.Mono.empty())
                .doFinally(signalType -> {
                    System.out.println(">>> [STREAM] Виконання схеми " + schema.schemaId + " завершено.");
                    processedOutputs.remove(schema.schemaId);
                })
                .subscribe();

        activeStreams.put(schema.schemaId, streamDisposable);
    }

    @SuppressWarnings("unchecked")
    private Flux<String> buildPipeline(Map<String, Flux<String>> currentInputMap, ComputationSchema schema) {
        Flux<String> currentOutputStream = currentInputMap.getOrDefault("default-input", currentInputMap.values().stream().findFirst().orElse(null));

        if (schema.pipelineStages != null) {
            for (ComputationSchema.PipelineStage stage : schema.pipelineStages) {

                if ("NETWORK_SINK".equalsIgnoreCase(stage.operation)) {
                    String streamId = "unknown-stream";
                    if (stage.parameters != null && stage.parameters.containsKey("streamId")) {
                        streamId = stage.parameters.get("streamId").toString();
                    }

                    if (currentOutputStream != null) {
                        System.out.println(">>> [STREAM] Зареєстровано публічний потік (Sink): " + streamId);
                        processedOutputs.put(streamId, currentOutputStream);
                    } else {
                        System.err.println(">>> [STREAM] Помилка: До NETWORK_SINK не підключено вхідних даних!");
                    }
                    continue;
                }

                // Безпечне приведення типу для підтримки Flux<String>
                MeshElement<String, String> element = (MeshElement<String, String>) (MeshElement<?, ?>) MeshElementFactory.create(stage.operation);
                element.connectInputStreams(currentInputMap);

                String outputPort = stage.operation.equals("CORRELATION") ? "tdoa-output" : "default-output";
                currentOutputStream = element.getOutputStream(outputPort);

                currentInputMap = Map.of("default-input", currentOutputStream);
            }
        }

        return currentOutputStream;
    }

    @Override
    public Flux<String> getProcessedStream(String sourceId) {
        if ("TODO".equals(sourceId) || sourceId.isEmpty()) {
            return processedOutputs.values().stream().findFirst().orElse(Flux.empty());
        }
        return processedOutputs.getOrDefault(sourceId, Flux.empty());
    }

    @Override
    public void stopAllStreams() {
        if (activeStreams.isEmpty()) {
            System.out.println(">>> [STREAM] Немає активних потоків для зупинки.");
            return;
        }

        activeStreams.forEach((targetNodeId, streamDisposable) -> {
            if (!streamDisposable.isDisposed()) {
                streamDisposable.dispose();
                System.out.println(">>> [STREAM] Потік з нодою " + targetNodeId + " зупинено.");
            }
        });

        activeStreams.clear();
        processedOutputs.clear();
    }

    private void stopStreamForNode(String nodeId) {
        Disposable stream = activeStreams.remove(nodeId);
        processedOutputs.remove(nodeId);
        if (stream != null && !stream.isDisposed()) {
            stream.dispose();
            System.out.println(">>> [STREAM] GC: Потік для мертвої ноди " + nodeId + " очищено.");
        }
    }
}