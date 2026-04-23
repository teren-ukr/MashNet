import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.util.Map;

public class NodeRequestHandler implements RSocket {
    private final Map<Integer, RSocket> connections;

    public NodeRequestHandler(Map<Integer, RSocket> connections) {
        this.connections = connections;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        if ("GREETING".equals(payload.getDataUtf8())) {
            int remotePort = Integer.parseInt(payload.getMetadataUtf8());
            System.out.println("\n[СЕРВЕР] Нода " + remotePort + " зареєструвалася у нас.");
            // Тут можна автоматично підключитися у відповідь, якщо треба (Mesh)
        }
        return Mono.empty();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        String command = payload.getDataUtf8();
        String senderId = payload.getMetadataUtf8();

        System.out.println("\n[СЕРВЕР] Отримано від [" + senderId + "]: " + command);

        return Mono.just(DefaultPayload.create("Команду " + command + " отримано сервером " + SimpleNode.port));
    }
}