package com.mashnet.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.mashnet.core.models.ComputationSchema;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.control.IStreamProvider;
import com.mashnet.network.topology.TopologyManager;
import com.mashnet.stream.math.IMathStrategy;
import com.mashnet.stream.math.MathStrategyFactory;
import com.mashnet.stream.sink.DataAggregatorSink;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

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
     * запускає всіпотоки даних
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


            //Дістаємо поточну схему з TopologyManager
            ComputationSchema currentSchema = topologyManager.getCurrentSchema();

            // Якщо схеми немає (хтось натиснув SENSOR до LOAD_SCHEMA), ставимо дефолт
            String operation = (currentSchema != null && currentSchema.operation != null)
                    ? currentSchema.operation
                    : "AVERAGE";

            //витянгуємо стратегію
            IMathStrategy strategy = mathFactory.getStrategy(operation);

            //id поточної ноди
            String myNodeId = topologyManager.getLocalNodeId();
            topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, true);

            var rawStream = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))
                    .map(payload -> Double.parseDouble(payload.getDataUtf8()));

            //передаємо стратегію
            Flux<Double> sharedStream = dataAggregator.processStream(rawStream, targetNodeId, strategy).share();



            // 3. Запускаємо потік
            Disposable streamDisposable = sharedStream
                    .doOnError(e -> System.err.println(">>> [STREAM] Зв'язок з нодою " + targetNodeId + " втрачено."))
                    .onErrorResume(e -> Mono.empty())
                    .doFinally(signalType -> {

                        // ПРИ ЗУПИНЦІ робимо потік неактивним
                        topologyManager.setStreamStatusAndBroadcast(targetNodeId, myNodeId, false);
                        // Очищаємо пам'ять при зупинці
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