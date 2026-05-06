package com.mashnet.network.control;

import io.rsocket.Payload;
import reactor.core.publisher.Mono;

/**
 * Базовий інтерфейс для команд типу Fire-And-Forget.
 */
public interface IFireAndForgetCommand {

    /**
     * Виконує логіку команди без повернення результату.
     *
     * @param payload     Вхідні дані від клієнта.
     * @param localNodeId Унікальний ID поточної ноди.
     * @return Mono<Void> сигнал про завершення операції (без даних).
     */
    Mono<Void> execute(Payload payload, String localNodeId);
}
