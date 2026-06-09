package com.mashnet.stream.elements;

import com.mashnet.stream.math.IMathStrategy;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;

public class MathOperationElement extends MeshElement<Double, Double> {

    private final IMathStrategy strategy;

    public MathOperationElement(IMathStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void connectInputStreams(Map<String, Flux<Double>> inputStreams) {
        // Для базових скалярних операцій беремо перший доступний вхід
        Flux<Double> mainInput = inputStreams.values().stream().findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Необхідно передати хоча б один вхідний потік"));

        Flux<Double> processedStream = mainInput
                .buffer(Duration.ofSeconds(1))
                .map(batch -> {
                    double result = strategy.calculate(batch);
                    System.out.println(">>> [ELEMENT: " + strategy.getOperationName() + "] Результат: " + result);
                    return result;
                })
                .share();

        // Реєструємо результат у стандартний вихідний порт
        this.outputStreams.put("default-output", processedStream);
    }
}