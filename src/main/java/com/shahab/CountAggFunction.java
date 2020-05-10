package com.shahab;

import java.io.Serializable;
import java.util.function.Function;

public class CountAggFunction<T> extends AggFunction<Long> implements Serializable {
    private Function<Student,T> mapper;

    public CountAggFunction( Function<Student,T> mapper){
        this.mapper = mapper;
    }

    @Override
    Long update(Long buffer, Student input) {
        if (mapper.apply(input) == null){
            return buffer;
        }
        return buffer+1;
    }

    @Override
    Long getInitialValue() {
        return 0L;
    }
}
