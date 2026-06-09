package com.mashnet.network.control;

import reactor.core.publisher.Flux;

/**
 * Абстракція для отримання реактивних потоків.
 * Дозволяє мережевому шару не залежати від модуля mash-stream.
 */
public interface IStreamProvider {
    Flux<Double> getProcessedStream(String sourceId);

    void startStreamFrom(String targetNodeId);
    void stopAllStreams();

    void startPipeline();
}

