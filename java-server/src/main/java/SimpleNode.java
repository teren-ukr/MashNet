import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.handler.ssl.util.SelfSignedCertificate;

class SimpleNode {

    private final Map<String, RSocket> connections = new ConcurrentHashMap<>();
    public final List<Integer> activeNodes = new CopyOnWriteArrayList<>();

    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public final String nodeId = "Node-" + UUID.randomUUID().toString().substring(0, 8);
    public int myPort;
    public boolean isServerStart = false;

    public static final int SEED_PORT = 7000;

    //------------------------------------------------------------------------------------------------------------------
    public static void main(String[] args) {
        var scanner = new Scanner(System.in);

        System.out.print("Мій порт (Сервер): ");
        int myPort = scanner.nextInt();


//        System.out.print("Порт сусіда (Клієнт): ");
//        int targetPort = scanner.nextInt();

        if (scanner.hasNextLine()) scanner.nextLine(); // Очистка буфера

        SimpleNode node = new SimpleNode();
        String command = "";

        while (!command.equals("EXIT")) {
            System.out.println("\n--- MENU ---");
            System.out.println(" START  - Запустити сервер");
            System.out.println(" CREATE - Запустити ноду (Сервер + Клієнт)");
            System.out.println(" SEND   - Відправити LOAD_SCHEMA");
            System.out.println(" PORTS  - Доступні ноди");
            System.out.println("EXIT      - Вихід");
            System.out.print("Вибір: ");

            command = scanner.nextLine().toUpperCase();

            switch (command) {

                case "START" -> {
                    node.startServer(myPort);

                    if(myPort != SEED_PORT)
                    {
                        System.out.println("Підключення до SeedNode (" + SEED_PORT + ")...");
                        node.runNode(myPort,SEED_PORT);
                    }
                }

                case "CREATE" -> {
                    System.out.print("port: ");
                    int p = scanner.nextInt();

                    // КРИТИЧНО: зчитуємо залишок рядка (\n), щоб він не потрапив у наступний цикл
                    if (scanner.hasNextLine()) scanner.nextLine();

                    node.runNode(myPort, p);
                }

                case "SEND" -> node.triggerLoadSchema();

                case "PORTS" -> {
                    System.out.println("Активні з'єднання (Node IDs): " + node.connections.keySet());
                    System.out.println("Відомі порти в мережі: " + node.activeNodes);

                }

                case "EXIT" -> System.out.println("Завершення...");

                default -> {
                    // Якщо команда порожня (просто натиснули Enter), не виводимо помилку
                    if (!command.isEmpty()) {
                        System.out.println("Невідома команда: [" + command + "]");
                    }
                }

            }

        }

    }

    //------------------------------------------------------------------------------------------------------------------
    /**
     * Запуск ноди: активує сервер та намагається підключитися до сусіда
     */
    void runNode(int myPort, int targetPort) {
        if (!isServerStart) {
            System.err.println("!!! Помилка: Сервер не запущено");
            return;
        }

        connectToNeighbor(targetPort)
                .subscribe(
                        rsocket -> {
                            // 1. Запитуємо ID у ноди, до якої підключилися
                            rsocket.requestResponse(DefaultPayload.create("WHO_ARE_YOU"))
                                    .subscribe(payload -> {
                                        // Отримуємо тільки ID (наприклад "Node-177b6f94")
                                        String remoteNodeId = payload.getDataUtf8();

                                        // Зберігаємо в мапу під чистим ID
                                        connections.put(remoteNodeId, rsocket);

                                        System.out.println("\n[OK] Підключено до ноди: " + remoteNodeId + " (порт " + targetPort + ")");
                                    });

                            // 3. Також повідомляємо їм про себе (як і було)
                            rsocket.fireAndForget(DefaultPayload.create("GREETING", String.valueOf(myPort)))
                                    .subscribe();
                        },
                        err -> System.err.println("\n[ERROR] Не вдалося з'єднатися: " + err.getMessage())
                );
    }

    //------------------------------------------------------------------------------------------------------------------
    public void startServer(int port) {
        this.myPort = port;
        try {
            // Для локального тестування генеруємо тимчасовий сертифікат автоматично
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            SslContext sslContext = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();

            TcpServer tcpServer = TcpServer.create()
                    .host("localhost")
                    .port(port)
                    .secure(ssl -> ssl.sslContext(sslContext)); // Вмикаємо шифрування

            RSocketServer.create()
                    .acceptor((setup, sendingSocket) -> {
                        String remoteNodeId = setup.getDataUtf8();
                        int remotePort = Integer.parseInt(setup.getMetadataUtf8());

                        System.out.println(">>> Підключилася нода: " + remoteNodeId + " на порту " + remotePort);

                        connections.put(remoteNodeId, sendingSocket);
                        if (!activeNodes.contains(remotePort)) activeNodes.add(remotePort);

                        // Передаємо контекст поточної ноди в обробник
                        return Mono.just(new NodeRequestHandler(nodeId, connections, activeNodes));
                    })
                    .resume(new io.rsocket.core.Resume().sessionDuration(Duration.ofMinutes(5)))
                    .bind(TcpServerTransport.create(tcpServer)) // Використовуємо наш захищений TcpServer
                    .subscribe(v -> System.out.println(">>> SSL Сервер [" + nodeId + "] слухає на порту " + port));

            isServerStart = true;
        } catch (Exception e) {
            System.err.println("!!! Помилка запуску SSL сервера: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    //Додано стійкість до підключень. Виконує багато спроб для підключення.
    public Mono<RSocket> connectToNeighbor(int port) {
        Payload setupPayload = DefaultPayload.create(nodeId, String.valueOf(myPort));

        try {
            SslContext sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();

            TcpClient tcpClient = TcpClient.create()
                    .host("localhost")
                    .port(port)
                    .secure(ssl -> ssl.sslContext(sslContext));

            return RSocketConnector.create()
                    .setupPayload(setupPayload)
                    // ДОДАЄМО ОБРОБНИК (ACCEPTOR) ДЛЯ КЛІЄНТА!
                    .acceptor(io.rsocket.SocketAcceptor.with(new NodeRequestHandler(nodeId, connections, activeNodes)))
                    .resume(new io.rsocket.core.Resume().sessionDuration(Duration.ofMinutes(5)))
                    .reconnect(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(2)))
                    .connect(TcpClientTransport.create(tcpClient));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    /**
     * Публічний метод для виклику відправки з меню
     */
    public void triggerLoadSchema() {
        if (connections.isEmpty()) {
            System.err.println("!!! Помилка: Немає активних підключень.");
            return;
        }

        System.out.println("Розсилка JSON LOAD_SCHEMA всім сусідам (" + connections.size() + ")...");

        // 1. Створюємо реальну схему
        ComputationSchema mySchema = new ComputationSchema(
                "task-math-01",
                "MULTIPLY",
                "port:7001",
                "port:7002"
        );

        try {
            // 2. Перетворюємо об'єкт у JSON строку
            String jsonSchema = JSON_MAPPER.writeValueAsString(mySchema);

            // 3. Відправляємо: Data = Назва команди, Metadata = JSON нашої схеми
            connections.forEach((targetNodeId, rsocket) -> {
                Payload payload = DefaultPayload.create("LOAD_SCHEMA", jsonSchema);

                rsocket.requestResponse(payload)
                        .subscribe(
                                res -> System.out.println("<<< Відповідь від " + targetNodeId + ": " + res.getDataUtf8()),
                                err -> System.err.println("!!! Помилка (нода " + targetNodeId + "): " + err.getMessage())
                        );
            });
        } catch (Exception e) {
            System.err.println("!!! Помилка створення JSON: " + e.getMessage());
        }
    }
}