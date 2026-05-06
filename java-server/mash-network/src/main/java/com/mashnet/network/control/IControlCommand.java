package com.mashnet.network.control;

import io.rsocket.Payload;
import reactor.core.publisher.Mono;

/**
 * Базовий інтерфейс для команд типу Request-Response (Запит-Відповідь).
 */
public interface IControlCommand {

   /**
    * Виконує логіку специфічної команди.
    *
    * @param payload     Вхідні дані від клієнта по RSocket (містить Data та Metadata).
    * @param localNodeId Унікальний ID поточної ноди (використовується для логування).
    * @return Mono<Payload> об'єкт реактивного потоку, що містить відповідь для клієнта.
    */
   Mono<Payload> execute(Payload payload, String localNodeId);
}
