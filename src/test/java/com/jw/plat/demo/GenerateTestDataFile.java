/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jw.plat.demo;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateTestDataFile {
    public static void main(String[] args) throws IOException {
        Random r = new Random();
        FileWriter fw = new FileWriter("D:/flink2/kk1");
        for(int i=1; i<=1000; i++) {
            fw.write(i+","+ RandomStringUtils.randomAlphanumeric(r.nextInt(10))+","+r.nextInt(100)+","+r.nextInt(10000)+"\n");
        }
        fw.flush();
        fw.close();

        FileWriter fw2 = new FileWriter("D:/flink2/kk2");
        for(int i=1; i<=2000; i++) {
            fw2.write(i+","+ RandomStringUtils.randomAlphanumeric(r.nextInt(10))+","+r.nextInt(100)+","+r.nextInt(10000)+"\n");
        }
        fw2.flush();
        fw2.close();

        FileWriter fw3 = new FileWriter("D:/flink2/kk3");
        for(int i=1; i<=1000000; i++) {
            fw3.write(i+","+ RandomStringUtils.randomAlphanumeric(r.nextInt(10))+","+r.nextInt(100)+","+r.nextInt(10000)+"\n");
        }
        fw3.flush();
        fw3.close();
    }

}
