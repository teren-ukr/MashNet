import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

public class NodeRequestHandler implements RSocket {

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        // Чисті дані (команда)
        String command = payload.getDataUtf8();

        // Службова інформація (хто надіслав)
        String senderId = payload.getMetadataUtf8();

        System.out.println("\n[СЕРВЕР] Отримано команду: " + command);
        System.out.println("[СЕРВЕР] Відправник (ID): " + senderId);

        String response = "Нода " + senderId + ", твій запит '" + command + "' оброблено!";
        return Mono.just(DefaultPayload.create(response));
    }
}