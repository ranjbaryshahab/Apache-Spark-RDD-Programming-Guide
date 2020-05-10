package com.shahab;

import java.io.Serializable;
import java.util.function.Function;

public class SumAggFunction extends AggFunction<Double> implements Serializable {
    private Function<Student,Double> mapper;

    public SumAggFunction( Function<Student,Double> mapper){
       this.mapper = mapper;
    }

    @Override
    public Double update( Double buffer, Student input) {
        return buffer + mapper.apply(input);
    }

    public Double getInitialValue(){
        return 0.0;
    }
}
