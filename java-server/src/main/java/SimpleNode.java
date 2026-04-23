import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Scanner;

class SimpleNode {

    private RSocket neighborSession; // Збережена сесія для відправки команд у будь-який момент
    public static int port;
    public static boolean isServerStart;

    static void main() {
        var scanner = new Scanner(System.in);

        System.out.print("Мій порт (Сервер): ");
        int myPort = scanner.nextInt();
        port = myPort;

//        System.out.print("Порт сусіда (Клієнт): ");
//        int targetPort = scanner.nextInt();

        if (scanner.hasNextLine()) scanner.nextLine(); // Очистка буфера

        SimpleNode node = new SimpleNode();
        String command = "";

        while (!command.equals("EXIT")) {
            System.out.println("\n--- MENU ---");
            System.out.println("1. START  - Запустити сервер");
            System.out.println("2. CREATE - Запустити ноду (Сервер + Клієнт)");
            System.out.println("3. SEND   - Відправити LOAD_SCHEMA");
            System.out.println("EXIT      - Вихід");
            System.out.print("Вибір: ");

            command = scanner.nextLine().toUpperCase();

            switch (command) {

                case "1", "START" -> {
                    startServer(myPort);
                }

                case "2", "CREATE" -> {
                    System.out.print("port: ");
                    int p = scanner.nextInt();

                    // КРИТИЧНО: зчитуємо залишок рядка (\n), щоб він не потрапив у наступний цикл
                    if (scanner.hasNextLine()) scanner.nextLine();

                    node.run(myPort, p);
                }

                case "3", "SEND" -> node.triggerLoadSchema();

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
    void run(int myPort, int targetPort) {
        // 1. перевірка сервера
        if (!isServerStart) {
            System.err.println("!!! Помилка: Сервер не запущено");
            return;
        }

        // 2. Встановлюємо з'єднання та зберігаємо його
        connectToNeighbor(targetPort)
                .subscribe(
                        rsocket -> {
                            this.neighborSession = rsocket;
                            System.out.println("\n[OK] З'єднання з сусідом встановлено і збережено!");
                        },
                        err -> System.err.println("\n[ERROR] Не вдалося з'єднатися: " + err.getMessage())
                );
    }

    //------------------------------------------------------------------------------------------------------------------
    private static void startServer(int port) {
        RSocketServer.create((_, _) -> Mono.just(new NodeRequestHandler()))
                // ДОДАЄМО ЦЕЙ БЛОК:
                .resume(new io.rsocket.core.Resume()
                        .sessionDuration(Duration.ofMinutes(5)))
                .bind(TcpServerTransport.create("localhost", port))
                .subscribe(_ -> System.out.println(">>> Сервер слухає на порту " + port));

        isServerStart = true;
    }

    //------------------------------------------------------------------------------------------------------------------
    //Додано стійкість до підключень. Виконує багато спроб для підключення.
    private Mono<RSocket> connectToNeighbor(int port) {

        return RSocketConnector.create()

                // Якщо з'єднання розірветься, намагатися відновити його протягом 5 хвилин
                .resume(new io.rsocket.core.Resume()
                        .sessionDuration(Duration.ofMinutes(5)))

                // Якщо сервера немає, пробувати підключитися кожні 2 секунди
                .reconnect(reactor.util.retry.Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(2))
                        .doBeforeRetry(retrySignal ->
                                System.out.println(">>> Спроба знайти сусіда на порту " + port + "... (Спроба " + (retrySignal.totalRetries() + 1) + ")")
                        ))
                .connect(TcpClientTransport.create("localhost", port));
    }

    //------------------------------------------------------------------------------------------------------------------
    /**
     * Публічний метод для виклику відправки з меню
     */
    public void triggerLoadSchema() {
        if (neighborSession == null) {
            System.err.println("!!! Помилка: Немає з'єднання.");
            return;
        }

        // Створюємо Payload: (Data, Metadata)
        // Data = "LOAD_SCHEMA", Metadata = "7006" (твій порт)
        Payload payload = DefaultPayload.create("LOAD_SCHEMA", String.valueOf(port));

        neighborSession.requestResponse(payload)
                .subscribe(
                        res -> System.out.println("<<< Відповідь: " + res.getDataUtf8()),
                        err -> System.err.println("!!! Помилка: " + err.getMessage())
                );
    }
}