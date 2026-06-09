package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;
import java.util.Map;

public interface IMeshSink<T> {
    /**
     * Підключає іменовані вхідні потоки до цього елемента обробки.
     * @param inputStreams Карта, де ключ — ідентифікатор входу (наприклад, "input-A"),
     * а значення — реактивний потік даних.
     */
    void connectInputStreams(Map<String, Flux<T>> inputStreams);
}