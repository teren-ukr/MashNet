package com.mashnet.network.control;

import com.mashnet.core.models.ComputationSchema;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.client.RsocketClientManager;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

/**
 * Команда для завантаження конфігурації обчислень (Схеми).
 * Приймає JSON із параметрами завдання і готує ноду до виконання.
 */
public class LoadSchemaCommand implements IControlCommand {

    public final RsocketClientManager clientManager;

    public LoadSchemaCommand(RsocketClientManager clientManager) {
        this.clientManager = clientManager;
    }

    @Override
    public Mono<Payload> execute(Payload payload, String localNodeId) {

        // Схема передається у Metadata, оскільки Data зайнята іменем команди ("LOAD_SCHEMA")
        String jsonSchema = payload.getMetadataUtf8();

        try {
            // Спроба розпарсити JSON у строгий типізований об'єкт Java
            ComputationSchema schema = JsonUtil.MAPPER.readValue(jsonSchema, ComputationSchema.class);
            System.out.println("[" + localNodeId + "] Схему [" + schema.schemaId + "] успішно завантажено.");

            // Якщо в схемі вказано, що джерело це порт (наприклад "port:8005")
            if(schema.inputSource.startsWith("port:")){
                int port = Integer.parseInt(schema.inputSource.replace("port:", ""));
                System.out.println("[" + localNodeId + "] Авто-підключення до джерела на порту " + port);

                // Використовуємо менеджер, щоб ініціювати TCP з'єднання
                clientManager.connectToNeighbor(port);
            }

            return Mono.just(DefaultPayload.create("SCHEMA_LOADED_OK"));

        } catch (Exception e) {
            System.err.println("[" + localNodeId + "] Помилка парсингу JSON схеми: " + e.getMessage());
            return Mono.just(DefaultPayload.create("ERROR: BAD_JSON"));
        }
    }
}