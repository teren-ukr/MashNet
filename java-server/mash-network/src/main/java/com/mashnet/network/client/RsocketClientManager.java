package com.mashnet.network.client;

import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.control.ControlCommandHandler;
import com.mashnet.network.topology.TopologyManager;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Клас відповідає за підключення до сусідніх нод (Клієнтська частина).
 * Містить логіку стійкості (Retry) та після-підключенне налаштування (Gossip, Greeting).
 */
public class RsocketClientManager {

    private final TopologyManager topologyManager;
    private final int localPort;

    public RsocketClientManager(TopologyManager topologyManager, int localPort) {
        this.topologyManager = topologyManager;
        this.localPort = localPort;
    }

    /**
     * Спроба підключитися до іншої ноди, дізнатися її ID та поширити плітку (Gossip) про новий зв'язок.
     *
     * @param targetPort порт ноди-сусіда.
     */
    public void connectToNeighbor(int targetPort) {
        System.out.println(">>> [CLIENT] Спроба підключення до порту " + targetPort + "...");

        createRawConnection(targetPort)
                .subscribe(
                        rsocket -> handleSuccessfulConnection(rsocket, targetPort),
                        err -> System.err.println("\n[ERROR] Не вдалося з'єднатися з портом " + targetPort + ": " + err.getMessage())
                );
    }

    /**
     * Низькорівневе створення сокета з налаштуванням SSL та Retry патерну (авто-відновлення).
     */
    private Mono<RSocket> createRawConnection(int port) {
        Payload setupPayload = DefaultPayload.create(topologyManager.getLocalNodeId(), String.valueOf(localPort));

        try {
            // Довіряємо самопідписаним сертифікатам
            SslContext sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            TcpClient tcpClient = TcpClient.create()
                    .host("localhost")
                    .port(port)
                    .secure(ssl -> ssl.sslContext(sslContext));

            return RSocketConnector.create()
                    .setupPayload(setupPayload)
                    // Наш клієнт теж повинен вміти обробляти команди від сервера!
                    .acceptor(io.rsocket.SocketAcceptor.with(new ControlCommandHandler(topologyManager)))
                    .resume(new io.rsocket.core.Resume().sessionDuration(Duration.ofMinutes(5)))
                    // TODO: Тут налаштоване авто-відновлення кожні 2 секунди
                    .reconnect(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(2)))
                    .connect(TcpClientTransport.create(tcpClient));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    /**
     * Бізнес-логіка після успішного встановлення фізичного з'єднання.
     */
    private void handleSuccessfulConnection(RSocket rsocket, int targetPort) {
        // 1. Запитуємо ID у ноди, до якої підключилися
        rsocket.requestResponse(DefaultPayload.create("WHO_ARE_YOU"))
                .subscribe(payload -> {
                    String remoteNodeId = payload.getDataUtf8();

                    // 2. Зберігаємо зв'язок
                    topologyManager.addConnection(remoteNodeId, rsocket);
                    System.out.println("\n[OK] Підключено до ноди: " + remoteNodeId + " (порт " + targetPort + ")");

                    // 3. Відправляємо GOSSIP (розказуємо всім про наш новий зв'язок)
                    broadcastNewEdge(remoteNodeId);
                });

        // 4. Вітаємося (передаємо свій порт для історії)
        rsocket.fireAndForget(DefaultPayload.create("GREETING", String.valueOf(localPort)))
                .subscribe();
    }

    /**
     * Формує JSON з інфою про зв'язок і відправляє його Seed-ноді (якщо ми з нею з'єднані).
     */
    private void broadcastNewEdge(String remoteNodeId) {
        try {
            Map<String, String> newEdge = new HashMap<>();
            newEdge.put("source", topologyManager.getLocalNodeId());
            newEdge.put("target", remoteNodeId);
            String edgeJson = JsonUtil.MAPPER.writeValueAsString(newEdge);

            // Якщо ми самі Seed, просто додаємо в пам'ять
            if (topologyManager.getLocalNodeId().startsWith("Seed-")) {
                topologyManager.addGlobalEdge(topologyManager.getLocalNodeId(), remoteNodeId);
            } else {
                // Шукаємо Seed серед наших активних підключень і відправляємо йому NEW_EDGE
                for (Map.Entry<String, RSocket> entry : topologyManager.getActiveConnections().entrySet()) {
                    if (entry.getKey().startsWith("Seed-")) {
                        entry.getValue().fireAndForget(DefaultPayload.create("NEW_EDGE", edgeJson)).subscribe();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Помилка GOSSIP (відправка NEW_EDGE): " + e.getMessage());
        }
    }
}