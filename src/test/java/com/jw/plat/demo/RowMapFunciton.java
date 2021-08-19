package com.jw.plat.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;

public class RowMapFunciton implements  MapFunction<String, Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>>{

    public Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception{
        String[]splits=s.split(",");
        Integer f1=0;
        try{f1=Integer.valueOf(splits[0]);}catch(Exception e){}
        Float f2=0f;
        try{f2=Float.valueOf(splits[1]);}catch(Exception e){}
        Integer f3=0;
        try{f3=Integer.valueOf(splits[2]);}catch(Exception e){}
        Integer f4=0;
        try{f4=Integer.valueOf(splits[3]);}catch(Exception e){}
        Float f5=0f;
        try{f5=Float.valueOf(splits[4]);}catch(Exception e){}
        Integer f6=0;
        try{f6=Integer.valueOf(splits[5]);}catch(Exception e){}
        Integer f7=0;
        try{f7=Integer.valueOf(splits[6]);}catch(Exception e){}
        Integer f8=0;
        try{f8=Integer.valueOf(splits[7]);}catch(Exception e){}
        Integer f9=0;
        try{f9=Integer.valueOf(splits[8]);}catch(Exception e){}
        Integer f10=0;
        try{f10=Integer.valueOf(splits[9]);}catch(Exception e){}
        Integer f11=0;
        try{f11=Integer.valueOf(splits[10]);}catch(Exception e){}

        return new Tuple11<>(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11);
        }
    }
