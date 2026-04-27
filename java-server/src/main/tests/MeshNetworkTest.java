import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeshNetworkTest {

    @Test
    void testNetworkLifecycleAndCommands() throws InterruptedException {
        System.out.println("--- 1. СТВОРЕННЯ МЕРЕЖІ ---");

        // Створюємо Seed Node (Головна точка входу)
        SimpleNode seedNode = new SimpleNode();
        seedNode.startServer(7000);

        // Створюємо 3 робочі ноди і підключаємо їх до Seed Node
        SimpleNode node1 = new SimpleNode();
        node1.startServer(7001);
        node1.connectToNeighbor(7000).block(); // Підключаємо до 7000

        SimpleNode node2 = new SimpleNode();
        node2.startServer(7002);
        node2.connectToNeighbor(7000).block(); // Підключаємо до 7000

        SimpleNode node3 = new SimpleNode();
        node3.startServer(7003);
        node3.connectToNeighbor(7000).block(); // Підключаємо до 7000

        // Даємо мережі 1-2 секунди на обмін привітаннями (GREETING)
        Thread.sleep(1500);

        // Перевіряємо, чи Seed Node побачила всі 3 підключення
        System.out.println("\n--- 2. ПЕРЕВІРКА ТОПОЛОГІЇ ---");
        System.out.println("Відомі порти Seed Node: " + seedNode.activeNodes);

        assertTrue(seedNode.activeNodes.contains(7001), "Нода 1 не підключилася");
        assertTrue(seedNode.activeNodes.contains(7002), "Нода 2 не підключилася");
        assertTrue(seedNode.activeNodes.contains(7003), "Нода 3 не підключилася");

        // Відправляємо команду всім нодам від Seed Node
        System.out.println("\n--- 3. ТЕСТУВАННЯ КЕРУЮЧОГО КАНАЛУ (COMMANDS) ---");
        seedNode.triggerLoadSchema();

        // Даємо час на обробку відповідей
        Thread.sleep(1000);

        System.out.println("\n--- ТЕСТ УСПІШНО ЗАВЕРШЕНО ---");
    }
}