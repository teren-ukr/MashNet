package com.mashnet.stream.math;

import java.util.Collections;
import java.util.List;

public class MaxStrategy implements IMathStrategy {
    @Override
    public String getOperationName() {
        return "MAX";
    }

    @Override
    public Double calculate(List<Double> batch) {
        if (batch == null || batch.isEmpty()) return 0.0;
        return Collections.max(batch);
    }
}