package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;

public interface IMeshSource<T> {
    /**
     * Повертає вихідний реактивний потік даних.
     */
    Flux<T> getOutputStream();
}