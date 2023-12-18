package com.advanced.spark.socket;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class Task implements Serializable {

    /**
     * 数据
     */
    private List<Integer> datas = new ArrayList<>(Arrays.asList(1,2,3,4));
    /**
     * 逻辑
     */
    private Function<Integer, Integer> function = item -> item * 2;

    public List<Integer> compute() {
        return datas.stream().map(function).collect(Collectors.toList());
    }

    public Task() {
    }
}
