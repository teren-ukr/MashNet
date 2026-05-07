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
        // --- Складні команди (Request-Response) винесені в окремі класи ---
        requestResponseCommands.put("GET_TOPOLOGY", new GetTopologyCommand(topologyManager));
        requestResponseCommands.put("LOAD_SCHEMA", new LoadSchemaCommand(clientManager));

        // --- Прості системні команди залишені як лямбда-вирази для компактності ---
        requestResponseCommands.put("WHO_ARE_YOU", (payload, nodeId) ->
                Mono.just(DefaultPayload.create(nodeId)));

        requestResponseCommands.put("START", (payload, nodeId) -> {
            System.out.println("[" + nodeId + "] -> Запуск обчислень згідно зі схемою...");
            return Mono.just(DefaultPayload.create("NODE_STARTED"));
        });

        requestResponseCommands.put("STOP", (payload, nodeId) -> {
            System.out.println("[" + nodeId + "] -> Зупинка поточних обчислень...");
            return Mono.just(DefaultPayload.create("NODE_STOPPED"));
        });

        requestResponseCommands.put("RESET", (payload, nodeId) -> {
            System.out.println("\n[" + nodeId + "] Скидання схеми обчислень...");
            return Mono.just(DefaultPayload.create("RESET_OK"));
        });

        // --- Команди без відповіді (Fire-And-Forget) ---
        fireAndForgetCommands.put("NEW_EDGE", new NewEdgeCommand(topologyManager));
    }

    /**
     * Обробка односторонніх повідомлень (наприклад, пліток про топологію).
     */
    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        String commandName = payload.getDataUtf8();
        IFireAndForgetCommand command = fireAndForgetCommands.get(commandName);

        // Якщо команду знайдено у реєстрі — виконуємо її
        if (command != null) {
            return command.execute(payload, topologyManager.getLocalNodeId());
        }

        // Якщо команда невідома, просто ігноруємо (fail-safe підхід)
        return Mono.empty();
    }

    /**
     * Обробка двосторонніх запитів (команда -> результат).
     */
    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        String commandName = payload.getDataUtf8();

        // Особлива логіка для LOAD_SCHEMA: оскільки сама схема лежить у Metadata,
        // клієнт може передати в Data щось на зразок LOAD_SCHEMA_123 тому перевіряємо по префіксу.
        if (commandName.startsWith("LOAD_SCHEMA")) {
            commandName = "LOAD_SCHEMA";
        }

        System.out.println("\n[" + topologyManager.getLocalNodeId() + "] Отримано команду: " + commandName);

        IControlCommand command = requestResponseCommands.get(commandName);

        if (command != null) {
            // Передаємо виконання конкретній стратегії
            return command.execute(payload, topologyManager.getLocalNodeId());
        }

        // Якщо команда невідома, повертаємо клієнту помилку
        System.err.println("[" + topologyManager.getLocalNodeId() + "] Невідома команда: " + commandName);
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
            System.out.println("[" + topologyManager.getLocalNodeId() + "] Дашборд підписався на живі події мережі!");
            // Транслюємо нашу шину подій (Sinks.Many) у реактивний потік Flux
            return topologyManager.getEventSink().asFlux().map(DefaultPayload::create);
        }

        // TODO: Додати обробку "START_SENSOR", коли буде створено StreamManager
        if ("START_SENSOR".equals(command)) {
            // У реальному коді тут треба шукати потрібний ID, поки що беремо заглушку "TODO"
            return streamProvider.getProcessedStream("TODO")
                    .map(value -> DefaultPayload.create(String.valueOf(value)));
        }


        return Flux.error(new IllegalArgumentException("Невідома команда потоку: " + command));
    }
}