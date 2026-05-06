package com.mashnet.network.control;

import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.topology.TopologyManager;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Команда для отримання поточної карти мережі.
 * Викликається Дашбордом для візуалізації вузлів та зв'язків.
 */
public class GetTopologyCommand implements IControlCommand {

    private final TopologyManager topologyManager;

    public GetTopologyCommand(TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
    }

    @Override
    public Mono<Payload> execute(Payload payload, String localNodeId) {
        try {
            // 1. Запитуємо актуальний зліпок графа (включаючи плітки)
            Map<String, Object> topologyMap = topologyManager.getTopologySnapshot();

            // 2. Конвертуємо Java Map у JSON-рядок через наш Singleton ObjectMapper
            String jsonTopology = JsonUtil.MAPPER.writeValueAsString(topologyMap);

            return Mono.just(DefaultPayload.create(jsonTopology));

        } catch (Exception e) {
            System.err.println("[" + localNodeId + "] Помилка генерації JSON топології: " + e.getMessage());
            // У разі критичної помилки повертаємо порожній граф, щоб не "покласти" UI дашборду
            return Mono.just(DefaultPayload.create("{\"nodes\":[], \"edges\":[]}"));
        }
    }
}