package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;
import java.util.HashMap;
import java.util.Map;

/**
 * Абстрактний блок обробки з підтримкою багатьох входів та виходів.
 */
public abstract class MeshElement<I, O> implements IMeshSink<I>, IMeshSource<O> {

    protected Map<String, Flux<O>> outputStreams = new HashMap<>();

    @Override
    public Map<String, Flux<O>> getOutputStreams() {
        if (outputStreams == null || outputStreams.isEmpty()) {
            throw new IllegalStateException("Вихідні потоки ще не ініціалізовано. Спочатку викличте connectInputStreams().");
        }
        return outputStreams;
    }

    /**
     * Допоміжний метод для отримання конкретного вихідного потоку за його ідентифікатором.
     */
    public Flux<O> getOutputStream(String portId) {
        Flux<O> stream = getOutputStreams().get(portId);
        if (stream == null) {
            throw new IllegalArgumentException("Вихідний порт з ідентифікатором '" + portId + "' не знайдено.");
        }
        return stream;
    }
}