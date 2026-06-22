package com.mashnet.network.control;

import com.mashnet.network.client.RsocketClientManager;
import com.mashnet.network.topology.TopologyManager;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

/**
 * Control Socket (Router / Dispatcher): Головний вузол обробки вхідних повідомлень.
 * Відповідає виключно за прийом команд по протоколу RSocket та перенаправлення їх
 * до відповідного класу-обробника (реалізація патерну Command / Strategy).
 * <p>
 * Цей клас НЕ містить бізнес-логіки. Якщо потрібно додати нову команду,
 * створюється новий клас, який реєструється в методі {@link #registerCommands()}.
 */
public class ControlCommandHandler implements RSocket {

    private final TopologyManager topologyManager;
    private final RsocketClientManager clientManager;
    private final IStreamProvider streamProvider;

    // Реєстри команд (Словники маршрутизації)
    private final Map<String, IControlCommand> requestResponseCommands = new HashMap<>();
    private final Map<String, IFireAndForgetCommand> fireAndForgetCommands = new HashMap<>();

    /**
     * Конструктор ініціалізує обробник та реєструє всі доступні команди.
     *
     * @param topologyManager посилання на глобальний стан мережі.
     */
    public ControlCommandHandler(TopologyManager topologyManager,
                                 RsocketClientManager clientManager,
                                 IStreamProvider streamProvider) {
        this.topologyManager = topologyManager;
        this.clientManager = clientManager;
        this.streamProvider = streamProvider;
        registerCommands();
    }

    /**
     * Реєстрація всіх підтримуваних команд у мережі.
     * Зв'язує текстовий ідентифікатор команди (String) з її обробником (Клас або Лямбда).
     */
    private void registerCommands() {
        requestResponseCommands.put("GET_TOPOLOGY", new GetTopologyCommand(topologyManager));
        requestResponseCommands.put("LOAD_SCHEMA", new LoadSchemaCommand(clientManager, topologyManager, streamProvider));

        requestResponseCommands.put("DEPLOY_SCHEMA", (payload, nodeId) -> {
            try {
                com.fasterxml.jackson.databind.JsonNode request = com.mashnet.core.utils.JsonUtil.MAPPER.readTree(payload.getMetadataUtf8());
                String targetId = request.get("targetNode").asText();
                String schemaJson = request.get("schema").toString();

                System.out.println("\n[DASHBOARD] Запит на розгортання схеми на вузлі: " + targetId);

                if (targetId.equals(topologyManager.getLocalNodeId())) {
                    Payload localPayload = DefaultPayload.create("LOAD_SCHEMA", schemaJson);
                    return new LoadSchemaCommand(clientManager, topologyManager, streamProvider).execute(localPayload, nodeId);
                } else {
                    RSocket targetSocket = topologyManager.getActiveConnections().get(targetId);
                    if (targetSocket != null) {
                        return targetSocket.requestResponse(DefaultPayload.create("LOAD_SCHEMA", schemaJson));
                    } else {
                        return Mono.just(DefaultPayload.create("ERROR: Вузол " + targetId + " не знайдено"));
                    }
                }
            } catch (Exception e) {
                return Mono.just(DefaultPayload.create("ERROR: " + e.getMessage()));
            }
        });

        fireAndForgetCommands.put("STOP_ALL", (payload, nodeId) -> {
            System.out.println("\n[DASHBOARD] Команда STOP_ALL. Зупиняємо потоки.");
            if (streamProvider != null) {
                streamProvider.stopAllStreams();
            }
            topologyManager.getActiveConnections().forEach((id, socket) -> {
                if(id.startsWith("JavaNode") || id.startsWith("Seed")) {
                    socket.requestResponse(DefaultPayload.create("STOP")).subscribe();
                }
            });
            return Mono.empty();
        });

        requestResponseCommands.put("WHO_ARE_YOU", (payload, nodeId) -> Mono.just(DefaultPayload.create(nodeId)));

        requestResponseCommands.put("START", (payload, nodeId) -> {
            System.out.println("[" + nodeId + "] -> Запуск обчислень згідно зі схемою...");
            return Mono.just(DefaultPayload.create("NODE_STARTED"));
        });

        requestResponseCommands.put("STOP", (payload, nodeId) -> {
            System.out.println("[" + nodeId + "] -> Зупинка поточних обчислень...");
            if (streamProvider != null) streamProvider.stopAllStreams();
            return Mono.just(DefaultPayload.create("NODE_STOPPED"));
        });

        requestResponseCommands.put("RESET", (payload, nodeId) -> {
            System.out.println("\n[" + nodeId + "] Скидання схеми обчислень...");
            topologyManager.setCurrentSchema(null);
            if (streamProvider != null) streamProvider.stopAllStreams();
            return Mono.just(DefaultPayload.create("RESET_OK"));
        });

        requestResponseCommands.put("REMOTE_CONNECT", (payload, nodeId) -> {
            try {
                // Читаємо JSON параметрів підключення з метаданих
                com.fasterxml.jackson.databind.JsonNode request = com.mashnet.core.utils.JsonUtil.MAPPER.readTree(payload.getMetadataUtf8());
                String fromNode = request.get("fromNode").asText();
                String toHost = request.get("toHost").asText();
                int toPort = request.get("toPort").asInt();

                System.out.println("\n[ORCHESTRATOR] Отримано наказ: Вузол [" + fromNode + "] має підключитися до [" + toHost + ":" + toPort + "]");

                // Перевіряємо, чи цей наказ призначений поточній ноді
                if (fromNode.equals(topologyManager.getLocalNodeId())) {
                    clientManager.connectToNeighbor(toHost, toPort);
                    return Mono.just(DefaultPayload.create("CONNECT_INITIATED_LOCCAL"));
                } else {
                    // Якщо це Seed, він шукає RSocket-з'єднання з нодою fromNode і пересилає наказ їй
                    RSocket targetSocket = topologyManager.getActiveConnections().get(fromNode);
                    if (targetSocket != null) {
                        return targetSocket.requestResponse(DefaultPayload.create("REMOTE_CONNECT", payload.getMetadataUtf8()));
                    } else {
                        return Mono.just(DefaultPayload.create("ERROR: Ноду відправника " + fromNode + " не знайдено в мережі"));
                    }
                }
            } catch (Exception e) {
                return Mono.just(DefaultPayload.create("ERROR: " + e.getMessage()));
            }
        });

        fireAndForgetCommands.put("NEW_EDGE", new NewEdgeCommand(topologyManager));
        fireAndForgetCommands.put("STREAM_EVENT", new StreamEventCommand(topologyManager));
    }

    /**
     * Обробка односторонніх повідомлень (наприклад, пліток про топологію).
     */
    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        String commandName = payload.getDataUtf8();
        IFireAndForgetCommand command = fireAndForgetCommands.get(commandName);

        if (command != null)
            return command.execute(payload, topologyManager.getLocalNodeId());

        return Mono.empty();
    }

    /**
     * Обробка двосторонніх запитів (команда -> результат).
     */
    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        String commandName = payload.getDataUtf8();

        if (commandName.startsWith("LOAD_SCHEMA"))
            commandName = "LOAD_SCHEMA";

        if (commandName.startsWith("DEPLOY_SCHEMA"))
            commandName = "DEPLOY_SCHEMA";

        // Додано перевірку для нової команди оркестрації
        if (commandName.startsWith("REMOTE_CONNECT"))
            commandName = "REMOTE_CONNECT";

        IControlCommand command = requestResponseCommands.get(commandName);

        if (command != null)
            return command.execute(payload, topologyManager.getLocalNodeId());

        return Mono.just(DefaultPayload.create("ERROR: UNKNOWN_COMMAND"));
    }
    /**
     * Відкриття постійного потоку даних (Stream).
     * Використовується для підписки на події мережі або запуску безперервних обчислень.
     */
    @Override
    public Flux<Payload> requestStream(Payload payload) {
        String command = payload.getDataUtf8();

        if ("SUBSCRIBE_EVENTS".equals(command)) {
            return topologyManager.getEventSink().asFlux().map(DefaultPayload::create);
        }

        if (command.startsWith("SUBSCRIBE_STREAM:")) {
            String payloadStr = command.replace("SUBSCRIBE_STREAM:", "");
            String targetNode = topologyManager.getLocalNodeId();
            String streamId = payloadStr;

            if (payloadStr.contains(":")) {
                String[] parts = payloadStr.split(":", 2);
                targetNode = parts[0];
                streamId = parts[1];
            }

            System.out.println("\n[ROUTER] Запит потоку [" + streamId + "] з вузла [" + targetNode + "]");

            if (targetNode.equals(topologyManager.getLocalNodeId()) || targetNode.equals("LOCAL")) {
                return streamProvider
                        .getProcessedStream(streamId)
                        .map(DefaultPayload::create);
            } else {
                // ПРОКСІЮВАННЯ ДО ІНШОЇ НОДИ
                RSocket targetSocket = topologyManager.getActiveConnections().get(targetNode);
                if (targetSocket != null) {
                    System.out.println(">>> [ROUTER] Перенаправлення потоку до " + targetNode);
                    // Важливо: якщо запит був на сирий сенсор, шлемо просто START_SENSOR
                    String proxyCmd = streamId.equals("START_SENSOR") ? "START_SENSOR" : "SUBSCRIBE_STREAM:LOCAL:" + streamId;
                    return targetSocket.requestStream(DefaultPayload.create(proxyCmd));
                } else {
                    System.err.println("!!! [ROUTER] Вузол " + targetNode + " не знайдено для проксіювання.");
                    return Flux.error(new RuntimeException("Вузол " + targetNode + " недоступний"));
                }
            }
        }

        if ("START_SENSOR".equals(command)) {
            return streamProvider.getProcessedStream("TODO").map(DefaultPayload::create);
        }

        return Flux.error(new IllegalArgumentException("Невідома команда потоку: " + command));
    }
}