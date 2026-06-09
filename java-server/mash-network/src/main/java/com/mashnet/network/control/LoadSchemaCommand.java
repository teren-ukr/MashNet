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

            // 1. Зберігаємо схему
            topologyManager.setCurrentSchema(schema);

            // 2. Перевіряємо наявність джерел та підключаємось за потреби
            if (schema.inputSources != null && !schema.inputSources.isEmpty()) {

                // Перевіряємо, чи є серед джерел вказівки на фізичне підключення до портів
                for (String sourceValue : schema.inputSources.values()) {
                    if (sourceValue != null && sourceValue.startsWith("port:")) {
                        int port = Integer.parseInt(sourceValue.replace("port:", ""));
                        clientManager.connectToNeighbor(port);
                    }
                }

                // 3. Запускаємо загальний конвеєр
                streamManager.startPipeline();
            }

            return Mono.just(DefaultPayload.create("SCHEMA_LOADED_OK"));
        } catch (Exception e) {
            System.err.println("[" + localNodeId + "] Помилка парсингу JSON схеми: " + e.getMessage());
            return Mono.just(DefaultPayload.create("ERROR: BAD_JSON"));
        }
    }
}