package com.mashnet.stream.math;

import java.util.HashMap;
import java.util.Map;

public class MathStrategyFactory {
    private final Map<String, IMathStrategy> strategies = new HashMap<>();

    //реєстрація всіх наявних стратегій
    public MathStrategyFactory(){
        registerStrategy(new AverageStrategy());
        registerStrategy(new MaxStrategy());
    }

    private void registerStrategy(IMathStrategy strategy)
    {
        strategies.put(strategy.getOperationName().toUpperCase(), strategy);
    }

    public IMathStrategy getStrategy(String operationName)
    {
        if(operationName == null)
        {
            return new AverageStrategy();
        }

        IMathStrategy strategy = strategies.get(operationName.toUpperCase());
        if(strategy == null)
        {
            System.err.println("[STRATEGIES] Увага: Невідома операція '" + operationName + "'. Використовується AVERAGE.");
            return new AverageStrategy();
        }
        return strategy;
    }
}
