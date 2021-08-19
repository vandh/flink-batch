package com.jw.plat.common.filter;

import org.apache.flink.api.common.functions.FilterFunction;

public class FilterCols implements FilterFunction<String>{
    @Override
    public boolean filter(String s) throws Exception {
        //System.out.println("..."+s);
        return s.split(",")[1].length()==5;
    }
}
