package com.mashnet.network.control;

import com.mashnet.core.models.ComputationSchema;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.client.RsocketClientManager;
import com.mashnet.network.topology.TopologyManager;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

/**
 * Команда для завантаження конфігурації обчислень (Схеми).
 * Приймає JSON із параметрами завдання і готує ноду до виконання.
 */
public class LoadSchemaCommand implements IControlCommand {

    private final RsocketClientManager clientManager;
    private final TopologyManager topologyManager;
    private final IStreamProvider streamManager;

    public LoadSchemaCommand(RsocketClientManager clientManager, TopologyManager topologyManager, IStreamProvider streamManager) {
        this.clientManager = clientManager;
        this.topologyManager = topologyManager;
        this.streamManager = streamManager;
    }

    @Override
    public Mono<Payload> execute(Payload payload, String localNodeId) {
        String jsonSchema = payload.getMetadataUtf8();

        try {
            ComputationSchema schema = JsonUtil.MAPPER.readValue(jsonSchema, ComputationSchema.class);
            System.out.println("[" + localNodeId + "] Схему [" + schema.schemaId + "] успішно завантажено.");

            // 1. ЗБЕРІГАЄМО СХЕМУ
            topologyManager.setCurrentSchema(schema);

            // 2. АВТО-ЗАПУСК ПОТОКУ АБО ПІДКЛЮЧЕННЯ
            if (schema.inputSource != null) {
                if (schema.inputSource.startsWith("NodePy")) {
                    // Якщо джерело - це Python сенсор, миттєво відкриваємо RSocket потік
                    streamManager.startStreamFrom(schema.inputSource);
                } else if (schema.inputSource.startsWith("port:")) {
                    int port = Integer.parseInt(schema.inputSource.replace("port:", ""));
                    clientManager.connectToNeighbor(port);
                }
            }

            return Mono.just(DefaultPayload.create("SCHEMA_LOADED_OK"));
        } catch (Exception e) {
            System.err.println("[" + localNodeId + "] Помилка парсингу JSON схеми: " + e.getMessage());
            return Mono.just(DefaultPayload.create("ERROR: BAD_JSON"));
        }
    }
}