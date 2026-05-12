package com.mashnet.stream.math;

import java.util.Collections;
import java.util.List;

public class MultiplyStrategy implements IMathStrategy{
    @Override
    public String getOperationName() {
        return "MULTIPLY";
    }

    @Override
    public Double calculate(List<Double> batch) {
        if(batch == null || batch.isEmpty()) return 0.0;

        double multipleRes = 1;
        for(var num : batch)
            multipleRes *= num;

        return multipleRes;
    }
}
