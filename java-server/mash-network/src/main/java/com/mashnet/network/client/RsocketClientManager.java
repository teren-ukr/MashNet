package com.mashnet.network.client;

import com.mashnet.core.utils.JsonUtil;
import com.mashnet.network.control.ControlCommandHandler;
import com.mashnet.network.control.IStreamProvider;
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

import java.io.File;
import java.time.Duration;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Клас відповідає за підключення до сусідніх нод (Клієнтська частина).
 * Містить логіку стійкості (Retry) та після-підключенне налаштування (Gossip, Greeting).
 */
public class RsocketClientManager {

    private final TopologyManager topologyManager;
    private final int localPort;
    private final IStreamProvider streamProvider;


    public RsocketClientManager(TopologyManager topologyManager, int localPort, IStreamProvider streamProvider) {
        this.topologyManager = topologyManager;
        this.localPort = localPort;
        this.streamProvider = streamProvider;
    }

    /**
     * Спроба підключитися до іншої ноди, дізнатися її ID та поширити плітку (Gossip) про новий зв'язок.
     *
     * @param targetPort порт ноди-сусіда.
     *
     * Старий метод (для зворотної сумісності з ConsoleMenu).
     * За замовчуванням підключається до localhost.
     */
    public void connectToNeighbor(int targetPort) {
        connectToNeighbor("127.0.0.1", targetPort);
    }

    /**
     * НОВИЙ метод: Спроба підключитися до іншої ноди за вказаним IP та портом.
     */
    public void connectToNeighbor(String targetHost, int targetPort) {
        System.out.println(">>> [CLIENT] Спроба підключення до " + targetHost + ":" + targetPort + "...");

        createRawConnection(targetHost, targetPort)
                .subscribe(
                        rsocket -> handleSuccessfulConnection(rsocket, targetPort),
                        err -> System.err.println("\n[ERROR] Не вдалося з'єднатися з " + targetHost + ":" + targetPort + ": " + err.getMessage())
                );
    }

    /**
     * Низькорівневе створення сокета з налаштуванням SSL та Retry патерну (авто-відновлення).
     */
    private Mono<RSocket> createRawConnection(String host, int port) {
        Payload setupPayload = DefaultPayload.create(topologyManager.getLocalNodeId(), String.valueOf(localPort));

        try {
            // Шляхи до НАШИХ сертифікатів (ми виступаємо як клієнт)
            File clientCert = new File("certs/server1.crt");
            File clientKey = new File("certs/server1.key");
            File caCert = new File("certs/ca.crt");

            // Налаштовуємо mTLS для клієнта
            SslContext sslContext = SslContextBuilder.forClient()
                    .keyManager(clientCert, clientKey) // Надаємо наш сертифікат для перевірки
                    .trustManager(caCert) // Перевіряємо сервер за допомогою CA
                    .build();

            // Використовуємо передану змінну host
            TcpClient tcpClient = TcpClient.create()
                    .host(host)
                    .port(port)
                    .secure(ssl -> ssl.sslContext(sslContext));

            return RSocketConnector.create()
                    .setupPayload(setupPayload)
                    .acceptor(io.rsocket.SocketAcceptor.with(new ControlCommandHandler(topologyManager, this, streamProvider)))
                    .resume(new io.rsocket.core.Resume().sessionDuration(Duration.ofMinutes(5)))
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