package com.mashnet.stream.math;

import java.util.List;

public class AverageStrategy implements IMathStrategy {
    @Override
    public String getOperationName() {
        return "AVERAGE";
    }

    @Override
    public Double calculate(List<Double> batch) {
        if (batch == null || batch.isEmpty()) return 0.0;

        double sum = 0;
        for (Double val : batch) {
            sum += val;
        }
        return sum / batch.size();
    }
}