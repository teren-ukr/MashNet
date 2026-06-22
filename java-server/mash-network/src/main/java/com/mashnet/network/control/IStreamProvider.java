package com.mashnet.network.control;

import reactor.core.publisher.Flux;

/**
 * Абстракція для отримання реактивних потоків.
 * Дозволяє мережевому шару не залежати від модуля mash-stream.
 */
public interface IStreamProvider {

    // Змінено з Double на String
    Flux<String> getProcessedStream(String sourceId);

    void startStreamFrom(String targetNodeId);
    void stopAllStreams();

    void startPipeline();
}

