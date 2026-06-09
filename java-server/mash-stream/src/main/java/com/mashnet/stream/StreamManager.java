package com.mashnet.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.mashnet.core.models.ComputationSchema;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.control.IStreamProvider;
import com.mashnet.network.topology.TopologyManager;
import com.mashnet.stream.elements.MathOperationElement;
import com.mashnet.stream.elements.MeshElement;
import com.mashnet.stream.elements.MeshElementFactory;
import com.mashnet.stream.math.IMathStrategy;
import com.mashnet.stream.math.MathStrategyFactory;
import com.mashnet.stream.sink.DataAggregatorSink;
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
    private final MathStrategyFactory mathFactory;

    // Реєстр активних "пультів керування" (Disposable)
    private final Map<String, Disposable> activeStreams = new ConcurrentHashMap<>();

    // Реєстр "гарячих" потоків з уже обчисленими даними для роздачі іншим
    private final Map<String, Flux<Double>> processedOutputs = new ConcurrentHashMap<>();

    public StreamManager(TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        this.dataAggregator = new DataAggregatorSink();
        this.mathFactory = new MathStrategyFactory();
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
                // Ігноруємо помилки парсингу системних подій
            }
        });
    }

    /**
     * запускає всі потоки даних
     */
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

            // Дістаємо поточну схему з TopologyManager
            ComputationSchema currentSchema = topologyManager.getCurrentSchema();

            // ID поточної ноди для широкомовного сповіщення
            String myNodeId = topologyManager.getLocalNodeId();
            topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, true);

            System.out.println(">>> [STREAM] Відкриваємо канал Request-Stream до " + targetNodeId);

            // 1. Отримуємо сирий потік від сенсора (наш Source)
            Flux<Double> rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                    .map(payload -> Double.parseDouble(payload.getDataUtf8()));

            // 2. Збираємо динамічний пайплайн (використовуємо спільний метод без дублювання коду)
            Map<String, Flux<Double>> inputMap = Map.of("default-input", rawStream);
            Flux<Double> sharedStream = buildPipeline(inputMap, currentSchema);

            // 3. Зберігаємо фінальний потік результатів для роздачі іншим нодам
            processedOutputs.put(targetNodeId, sharedStream);

            // 4. Запускаємо потік (Підписуємось)
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


    /**
     * Запускає потік від конкретного сенсора.
     */
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

        // 1. Отримуємо сирий потік від сенсора (наш Source)
        Flux<Double> rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                .map(payload -> Double.parseDouble(payload.getDataUtf8()));

        // 2. Збираємо динамічний пайплайн
        ComputationSchema currentSchema = topologyManager.getCurrentSchema();
        Map<String, Flux<Double>> inputMap = Map.of("default-input", rawStream);
        Flux<Double> sharedStream = buildPipeline(inputMap, currentSchema);

        // 3. Зберігаємо фінальний потік результатів для роздачі іншим нодам
        processedOutputs.put(targetNodeId, sharedStream);

        // 4. Запускаємо потік (Підписуємось)
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

    /**
     * Динамічний запуск графа на основі завантаженої схеми.
     */
    public void startPipeline() {
        ComputationSchema schema = topologyManager.getCurrentSchema();
        if (schema == null || schema.inputSources == null || schema.inputSources.isEmpty()) {
            System.err.println(">>> [STREAM] Схема не містить джерел даних.");
            return;
        }

        Map<String, Flux<Double>> sourceStreams = new HashMap<>();
        String myNodeId = topologyManager.getLocalNodeId();

        // 1. Встановлюємо з'єднання з кожним сенсором
        for (Map.Entry<String, String> entry : schema.inputSources.entrySet()) {
            String portId = entry.getKey();
            String targetNodeId = entry.getValue();

            RSocket rsocket = topologyManager.getActiveConnections().get(targetNodeId);

            if (rsocket != null) {
                topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, true);

                Flux<Double> rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                        .map(payload -> Double.parseDouble(payload.getDataUtf8()))
                        .doOnError(e -> System.err.println(">>> [STREAM] Помилка потоку від " + targetNodeId));

                sourceStreams.put(portId, rawStream);
                System.out.println(">>> [STREAM] Порт [" + portId + "] підключено до вузла [" + targetNodeId + "]");
            } else {
                System.err.println(">>> [STREAM] УВАГА: Вузол " + targetNodeId + " недоступний!");
            }
        }

        if (sourceStreams.isEmpty()) return;

        // 2. Збираємо пайплайн
        Flux<Double> finalStream = buildPipeline(sourceStreams, schema);

        // 3. Зберігаємо фінальний потік та підписуємось на нього
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


    /**
     * Конструює ланцюжок обробки.
     */
    private Flux<Double> buildPipeline(Map<String, Flux<Double>> currentInputMap, ComputationSchema schema) {
        Flux<Double> currentOutputStream = null;

        if (schema.pipelineStages != null) {
            for (ComputationSchema.PipelineStage stage : schema.pipelineStages) {

                // Патерн Factory Method для створення вузлів
                MeshElement<Double, Double> element = MeshElementFactory.create(stage.operation);

                element.connectInputStreams(currentInputMap);

                // Визначаємо вихідний порт (для кореляції це 'tdoa-output', для інших 'default-output')
                String outputPort = stage.operation.equals("CORRELATION") ? "tdoa-output" : "default-output";
                currentOutputStream = element.getOutputStream(outputPort);

                // Передаємо результат на наступний етап
                currentInputMap = Map.of("default-input", currentOutputStream);
            }
        }

        return currentOutputStream;
    }


    /**
     * Реалізація інтерфейсу IStreamProvider.
     * Віддає вже існуючий потік обчислень для передачі по мережі.
     */
    @Override
    public Flux<Double> getProcessedStream(String sourceId) {
        // Якщо ми просто хочемо взяти перший ліпший потік (заглушка "TODO" з ControlCommandHandler)
        if ("TODO".equals(sourceId) || sourceId.isEmpty()) {
            return processedOutputs.values().stream().findFirst().orElse(Flux.empty());
        }
        // Або шукаємо по конкретному ID ноди
        return processedOutputs.getOrDefault(sourceId, Flux.empty());
    }

    /**
     * Зупиняє всі активні потоки.
     * Обов'язкова реалізація методу з інтерфейсу IStreamProvider.
     */
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
        processedOutputs.clear(); // Очищаємо буфер потоків
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