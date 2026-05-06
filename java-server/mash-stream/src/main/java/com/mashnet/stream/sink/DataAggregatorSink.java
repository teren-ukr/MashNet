package com.mashnet.stream.sink;

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
    public Flux<Double> processStream(Flux<Double> sourceStream, String sourceNodeId) {
        return sourceStream
                // Збираємо всі значення за 1 секунду в список (batch)
                .buffer(Duration.ofSeconds(1))
                // Обробляємо кожну пачку даних
                .map(batch -> {
                    if (batch.isEmpty()) return 0.0;

                    double sum = 0;
                    for (Double temp : batch) {
                        sum += temp;
                    }
                    double avg = sum / batch.size();

                    System.out.printf("[ОБЧИСЛЕННЯ %s] Отримано значень: %d. Середня температура: %.2f °C%n",
                            sourceNodeId, batch.size(), avg);

                    return avg;
                });
    }
}