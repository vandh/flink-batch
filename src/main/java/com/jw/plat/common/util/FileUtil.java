package com.jw.plat.common.util;

import java.io.File;

public class FileUtil {
    public static final String getFileName(String path, String file, String batch) {
        File dir = new File(path);
        File[] files = dir.listFiles();
        for(File f : files) {
            if(f.getName().toLowerCase().indexOf(file.toLowerCase())!=-1) {
                if(batch!=null && f.getName().endsWith("_0"+batch))
                    return f.getPath();
            }
        }

        throw new RuntimeException(path+" no designed file :"+file+"_0"+batch);
    }

    public static void main(String[] args) {
        FileUtil.getFileName("D:\\flink", "GL_JE_LINES_POST", "1");
    }
}
