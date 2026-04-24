import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

public class NodeRequestHandler implements RSocket {

    private final String myNodeId;
    private final Map<String, RSocket> connections;
    private final List<Integer> activeNodes;

    // Конструктор тепер приймає стан конкретної ноди
    public NodeRequestHandler(String myNodeId, Map<String, RSocket> connections, List<Integer> activeNodes) {
        this.myNodeId = myNodeId;
        this.connections = connections;
        this.activeNodes = activeNodes;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        if ("GREETING".equals(payload.getDataUtf8())) {
            String metadata = payload.getMetadataUtf8();

            if (metadata != null && !metadata.isEmpty()) {
                int remotePort = Integer.parseInt(metadata);

                if (!activeNodes.contains(remotePort)) {
                    activeNodes.add(remotePort);
                    System.out.println("\n[INFO] Зареєстровано нову ноду: " + remotePort);
                }
            }
        }
        return Mono.empty();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        String command = payload.getDataUtf8();

        if ("WHO_ARE_YOU".equals(command)) {
            return Mono.just(DefaultPayload.create(myNodeId));
        }

        System.out.println("\n[" + myNodeId + "] Отримано команду на керуючому каналі: " + command);

        // Реалізація системи команд із ТЗ
        if (command.startsWith("LOAD_SCHEMA")) {
            String jsonSchema = payload.getMetadataUtf8();
            System.out.println("\n[" + myNodeId + "] Отримано JSON схеми: " + jsonSchema);

            try {
                // Перетворюємо JSON назад у Java об'єкт!
                ComputationSchema schema = SimpleNode.JSON_MAPPER.readValue(jsonSchema, ComputationSchema.class);

                System.out.println("[" + myNodeId + "] Успішно розпарсено!");
                System.out.println("   -> ID: " + schema.schemaId);
                System.out.println("   -> Операція: " + schema.operation);
                System.out.println("   -> Джерело: " + schema.inputSource);

                // TODO: Тут ми маємо зберегти цю схему в пам'ять ноди (наприклад, у поле класу)

                return Mono.just(DefaultPayload.create("SCHEMA_LOADED_OK"));
            } catch (Exception e) {
                System.err.println("[" + myNodeId + "] Помилка парсингу JSON: " + e.getMessage());
                return Mono.just(DefaultPayload.create("ERROR: BAD_JSON"));
            }
        }
        else if ("START".equals(command)) {
            System.out.println("-> Запуск обчислень згідно зі схемою...");
            // Тут буде логіка встановлення Data-з'єднань
            return Mono.just(DefaultPayload.create("NODE_STARTED"));
        }
        else if ("STOP".equals(command)) {
            System.out.println("-> Зупинка поточних обчислень...");
            return Mono.just(DefaultPayload.create("NODE_STOPPED"));
        }
        else if ("RESET".equals(command)) {
            System.out.println("-> Очищення пам'яті та стану ноди...");
            return Mono.just(DefaultPayload.create("NODE_RESET"));
        }

        return Mono.just(DefaultPayload.create("ERROR: UNKNOWN_COMMAND"));
    }
}