package com.mashnet.stream.math;

import java.util.List;

/**
 * Інтерфейс для математичних стратегій обробки потоку даних.
 */
public interface IMathStrategy {

    /**
     * Повертає назву операції, яку підтримує ця стратегія (наприклад, "AVERAGE", "MAX").
     */
    String getOperationName();

    /**
     * Обчислює результат на основі буфера даних (пачки за 1 секунду).
     */
    Double calculate(List<Double> batch);
}