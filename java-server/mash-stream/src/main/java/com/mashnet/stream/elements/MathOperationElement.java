package com.mashnet.stream.elements;

import com.mashnet.stream.math.IMathStrategy;
import reactor.core.publisher.Flux;
import java.time.Duration;

public class MathOperationElement extends MeshElement<Double, Double> {

    private final IMathStrategy strategy;

    public MathOperationElement(IMathStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void connectInputStream(Flux<Double> inputStream) {
        this.outputStream = inputStream
                // Збираємо дані в пачки за 1 секунду
                .buffer(Duration.ofSeconds(1))
                .map(batch -> {
                    double result = strategy.calculate(batch);
                    System.out.println(">>> [ELEMENT: " + strategy.getOperationName() + "] Результат: " + result);
                    return result;
                })
                // .share() дозволяє кільком "слухачам" підключитися до цього виходу
                .share();
    }
}