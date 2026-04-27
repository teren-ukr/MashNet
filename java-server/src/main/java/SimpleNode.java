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
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
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
    private final Map<String, Disposable> activeStreams = new ConcurrentHashMap<>();

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
            System.out.println(" SENSOR - Відкрити потік та почати обчислення");
            System.out.println(" STOP   - Примусово зупинити всі потоки даних");
            System.out.println(" RESET  - Скинути налаштування підключених нод");
            System.out.println("EXIT     - Вихід");
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

                    // зчитуємо залишок рядка (\n), щоб він не потрапив у наступний цикл
                    if (scanner.hasNextLine()) scanner.nextLine();

                    System.out.println(">>> Спроба підключення до ноди " + Integer.toString(p));
                    node.runNode(myPort, p);
                }

                case "SEND" -> node.triggerLoadSchema();

                case "PORTS" -> {
                    System.out.println("Активні з'єднання (Node IDs): " + node.connections.keySet());
                    System.out.println("Відомі порти в мережі: " + node.activeNodes);

                }

                case "SENSOR" -> node.startSensorStream();
                case "STOP" -> node.stopSensorStream();
                case "RESET" -> node.triggerReset();

                case "EXIT" -> System.out.println("Завершення...");

                default -> {
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

                            // 3. повідомляємо їм про себе
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
                    .bind(TcpServerTransport.create(tcpServer)) // використовуємо наш захищений TcpServer
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

    //------------------------------------------------------------------------------------------------------------------
    //Метод запускає потік даних з датчиків і робить математичі обчислення над даними.
    //В даній функції просто вираховує середнє значення температури
    public void startSensorStream(){
        if(connections.isEmpty()){
            System.err.println("!!!ПомилкаЖ Нема активних підключень");
            return;
        }

        System.out.println("Запит потоку START_SENSOR від усіх підключених нод...");

        connections.forEach((targetNodeId, rsocket) ->{

            //Відправка команди START_SENSOR
            // streamDisposable це тип Disposable який являє собою пульт керування потом
           Disposable streamDisposable = rsocket.requestStream(DefaultPayload.create("START_SENSOR"))

                    //кастуємо байти в дробові числа
                    .map(payload -> Double.parseDouble(payload.getDataUtf8()))

                    //збираємо всі значення за секунду в пачку даних і рахуємо середнє значення
                    .buffer(Duration.ofSeconds(1))

                    //Обробка кожної пачки даних
                    .doOnNext(batch ->{
                        if(batch.isEmpty()){
                            return;
                        }

                        double sum = 0;
                        for(Double tempp : batch)
                            sum+=tempp;
                        double avg = sum / batch.size();

                        // Виводимо красиву статистику
                        System.out.printf("[ОБЧИСЛЕННЯ %s] Отримано значень за секунду: %d. Середня температура: %.2f °C%n",
                                targetNodeId, batch.size(), avg);

                    })
                    .doOnTerminate(() -> System.out.println(">>> Потік з нодою " + targetNodeId + " завершено."))
                    .doOnError(e -> System.err.println(">>> Зв'язок з нодою " + targetNodeId + " втрачено: " + e.getMessage()))
                    //Коли вимикається датчик то хаваємо цю помилку
                    .onErrorResume(e -> Mono.empty())
                    // АВТОНОМНІСТЬ: видалємо ноду зі списку при помилці чи завершенні
                    //треба обробляти singleType причини завершення, бо при паузі воно потік просто видаляє
                    .doFinally(signalType -> {
                        if(signalType == SignalType.CANCEL){
                            //ми самі зупиняємо потік і не треба його видаляти
                            System.out.println(">>> [СИСТЕМА] Потік призупинено. З'єднання з нодою " + targetNodeId + " залишається відкритим.");
                        }
                        else {
                            //датчик вмер або обрив мережі
                            System.out.println(">>> [СИСТЕМА] Видалення " + targetNodeId + " зі списку активних з'єднань.");
                            connections.remove(targetNodeId);
                        }


                    })
                    // Активуємо потік (відкриваємо вентиль)
                    .subscribe();

           //зберігаємо пульт керування потоком в мачив потоків
           activeStreams.put(targetNodeId, streamDisposable);

        });
    }

    //------------------------------------------------------------------------------------------------------------------
    //команда STOP яка просто перериває через Disposable потоків самі потоки.
    //На датчиках спрацює метод cancel() і він перейде у стан очікування.
    //його прикол в тому, що він відправляє системний кадр на датчик, без додаткових труднощів з JSON командами
    public void stopSensorStream(){
        if(activeStreams.isEmpty())
        {
            System.out.println(">>> Немає активних потоків для зупинки");
            return;
        }

        activeStreams.forEach((targetNodeIt, streamDisposable) -> {
            if(!streamDisposable.isDisposed()){
                //зупинка потоку
                streamDisposable.dispose();
                System.out.println(">>> Потік з ноди" + targetNodeIt + " зупинено");
            }
        });

        activeStreams.clear();
    }

    //------------------------------------------------------------------------------------------------------------------
    public void triggerReset() {
        if (connections.isEmpty()) {
            System.err.println("!!! Помилка: Немає активних підключень.");
            return;
        }

        System.out.println("Розсилка команди RESET всім сусідам (" + connections.size() + ")...");

        connections.forEach((targetNodeId, rsocket) -> {
            // Відправляємо просту команду без JSON
            rsocket.requestResponse(DefaultPayload.create("RESET"))
                    .subscribe(
                            res -> System.out.println("<<< Відповідь від " + targetNodeId + ": " + res.getDataUtf8()),
                            err -> System.err.println("!!! Помилка скидання (нода " + targetNodeId + "): " + err.getMessage())
                    );
        });
    }


}