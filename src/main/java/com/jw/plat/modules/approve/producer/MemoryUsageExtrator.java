package com.jw.plat.modules.approve.producer;

import java.lang.management.ManagementFactory;
import java.util.Random;

public class MemoryUsageExtrator {
    private static Random random = new Random(1000);
    public static long currentFreeMemorySizeInBytes() {
        return random.nextInt(10);
    }
}
