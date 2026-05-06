package com.mashnet.network.control;

import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.topology.TopologyManager;
import io.rsocket.Payload;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Команда обробки Gossip Protocol.
 * Отримує повідомлення про новий зв'язок між іншими вузлами мережі.
 */
public class NewEdgeCommand implements IFireAndForgetCommand {

    private final TopologyManager topologyManager;

    public NewEdgeCommand(TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
    }

    @Override
    public Mono<Void> execute(Payload payload, String localNodeId) {
        try {
            // Очікуваний формат: {"source": "NodeA", "target": "NodeB"}
            String edgeJson = payload.getMetadataUtf8();

            // Використовуємо універсальний Map, щоб не створювати окремий DTO клас для однієї операції
            Map<String, String> edge = JsonUtil.MAPPER.readValue(edgeJson, Map.class);

            // Реєструємо знахідку в глобальному реєстрі топології
            topologyManager.addGlobalEdge(edge.get("source"), edge.get("target"));

        } catch (Exception e) {
            System.err.println("[" + localNodeId + "] Помилка обробки NEW_EDGE: " + e.getMessage());
        }

        // FireAndForget завжди має повертати Mono.empty(), оскільки клієнт не чекає відповіді
        return Mono.empty();
    }
}