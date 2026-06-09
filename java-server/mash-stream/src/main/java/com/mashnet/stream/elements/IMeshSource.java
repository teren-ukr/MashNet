package com.mashnet.stream.elements;

import reactor.core.publisher.Flux;
import java.util.Map;

public interface IMeshSource<T> {
    /**
     * Повертає карту вихідних реактивних потоків даних.
     */
    Map<String, Flux<T>> getOutputStreams();
}