package com.mashnet.network.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.topology.TopologyManager;
import io.rsocket.Payload;
import reactor.core.publisher.Mono;

public class StreamEventCommand implements IFireAndForgetCommand {

    private final TopologyManager topologyManager;

    public StreamEventCommand(TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
    }


    @Override
    public Mono<Void> execute(Payload payload, String localNodeId) {
        try {
            String json = payload.getMetadataUtf8();
            JsonNode eventNode = JsonUtil.MAPPER.readTree(json);

            // Дістаємо обидва кінці труби
            String dataSource = eventNode.get("dataSource").asText();
            String dataTarget = eventNode.get("dataTarget").asText();
            boolean isActive = eventNode.get("isActive").asBoolean();

            topologyManager.setStreamStatusAndBroadcast(dataSource, dataTarget, isActive);

        } catch (Exception e) {
            System.err.println("[" + localNodeId + "] Помилка обробки STREAM_EVENT: " + e.getMessage());
        }
        return Mono.empty();
    }
}
