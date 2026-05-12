package com.mashnet.stream.sink;

import com.mashnet.stream.math.IMathStrategy;
import reactor.core.publisher.Flux;
import java.time.Duration;

/**
 * Sink (Приймач): Відповідає виключно за обробку вхідного потоку даних.
 * Відокремлює бізнес-логіку (математику) від мережевого шару.
 */
public class DataAggregatorSink {

    /**
     * Приймає сирий потік чисел, групує їх по секундах і рахує середнє.
     *
     * @param sourceStream Потік вхідних даних (Double)
     * @param sourceNodeId ID ноди, від якої йдуть дані (для логування)
     * @return Оброблений потік (Flux)
     */
    public Flux<Double> processStream(Flux<Double> sourceStream, String sourceNodeId, IMathStrategy strategy) {
        return sourceStream
                // Збираємо всі значення за 1 секунду в список (batch)
                .buffer(Duration.ofSeconds(1))
                // Обробляємо кожну пачку даних
                .map(batch -> {

                    Double result = strategy.calculate(batch);

                    System.out.printf("[ОБЧИСЛЕННЯ %s] Отримано: %d. Операція: %s. Результат: %.2f%n",
                            sourceNodeId, batch.size(), strategy.getOperationName(), result);

                    return result;
                });
    }
}