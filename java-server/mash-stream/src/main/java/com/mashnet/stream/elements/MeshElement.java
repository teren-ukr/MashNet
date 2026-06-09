package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;

/**
 * Абстрактний блок обробки.
 * Приймає потік типу I (Input), трансформує його і віддає потік типу O (Output).
 */
public abstract class MeshElement<I, O> implements IMeshSink<I>, IMeshSource<O> {

    protected Flux<O> outputStream;

    @Override
    public Flux<O> getOutputStream() {
        if (outputStream == null) {
            throw new IllegalStateException("Вихідний потік ще не ініціалізовано. Спочатку викличте connectInputStream().");
        }
        return outputStream;
    }
}