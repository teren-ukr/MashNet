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

        if ("SUBSCRIBE_EVENTS".equals(command))
            return topologyManager
                    .getEventSink()
                    .asFlux()
                    .map(DefaultPayload::create);

        if ("START_SENSOR".equals(command))
            return streamProvider
                    .getProcessedStream("TODO")
                    .map(value -> DefaultPayload.create(String.valueOf(value)));

        return Flux.error(new IllegalArgumentException("Невідома команда потоку: " + command));
    }
}