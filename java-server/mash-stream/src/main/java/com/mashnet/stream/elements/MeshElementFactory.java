package com.mashnet.stream.elements;

import com.mashnet.stream.math.MathStrategyFactory;

public class MeshElementFactory {

    private static final MathStrategyFactory mathFactory = new MathStrategyFactory();

    // Змінюємо тип повернення на MeshElement<?, ?>
    public static MeshElement<?, ?> create(String operation) {
        if ("CORRELATION".equalsIgnoreCase(operation) || "GCC_PHAT".equalsIgnoreCase(operation)) {
            return new CorrelationElement();
        }

        // Поведінка за замовчуванням - використання стратегій для SISO (Double)
        return new MathOperationElement(mathFactory.getStrategy(operation));
    }
}