package com.shahab;

import java.io.Serializable;

abstract class AggFunction<T>  {
    abstract T update(T buffer, Student input);
    abstract T getInitialValue();
}
