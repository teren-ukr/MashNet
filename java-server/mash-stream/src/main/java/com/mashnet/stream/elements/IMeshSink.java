package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;

public interface IMeshSink<T> {
    /**
     * Підключає вхідний потік до цього елемента.
     */
    void connectInputStream(Flux<T> inputStream);
}