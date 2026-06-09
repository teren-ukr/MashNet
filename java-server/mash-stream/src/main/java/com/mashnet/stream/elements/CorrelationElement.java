package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Елемент DSP-графа для розрахунку взаємнокореляційної функції між двома сигналами.
 */
public class CorrelationElement extends MeshElement<Double, Double> {

    @Override
    public void connectInputStreams(Map<String, Flux<Double>> inputStreams) {
        // Очікуємо конкретні ідентифікатори портів згідно з JSON-контрактом Дашборду
        Flux<Double> streamA = inputStreams.get("input-A");
        Flux<Double> streamB = inputStreams.get("input-B");

        if (streamA == null || streamB == null) {
            throw new IllegalArgumentException("CorrelationElement вимагає наявності потоків на портах 'input-A' та 'input-B'");
        }

        // Flux.zip утворює пари значень (Tuple2), блокуючи швидший потік до отримання даних з повільнішого
        Flux<Double> correlationStream = Flux.zip(streamA, streamB)
                .buffer(Duration.ofSeconds(1))
                .map(this::calculateCrossCorrelation)
                .share();

        // Реєструємо вихідний потік з результатом пеленгації
        this.outputStreams.put("tdoa-output", correlationStream);
    }

    /**
     * Математичний апарат обчислення різниці часу приходу (TDOA).
     */
    private Double calculateCrossCorrelation(List<Tuple2<Double, Double>> synchronizedBatch) {
        if (synchronizedBatch == null || synchronizedBatch.isEmpty()) {
            return 0.0;
        }

        // TODO: Інтеграція алгоритму GCC-PHAT (Узагальнена взаємна кореляція з фазовою трансформацією).
        // Для поточного прототипу реалізуємо заглушку, яка підтверджує факт синхронної обробки.

        double mockDelayEstimate = 0.0;

        System.out.println(">>> [CORRELATION] Оброблено синхронізований кадр розміром: "
                + synchronizedBatch.size() + " семплів.");

        return mockDelayEstimate;
    }
}