package com.dawson.bigdata.offlineDw;

import com.dawson.bigdata.offlineDw.exception.EtlException;

public class EtlCommonApp {

    public static final String setupMethod = "setup";
    public static final String eltMethod = "etl";
    public static final String cleanupMethod = "cleanup";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new EtlException("必须传入一个要执行的类名！");
        }

        String className = args[0];

        Class<?> clazz = Class.forName(className);
        Object instance = clazz.getDeclaredConstructor().newInstance();

        try {
            clazz.getMethod(setupMethod).invoke(instance);

            clazz.getMethod(eltMethod).invoke(instance);
        } finally {
            clazz.getMethod(cleanupMethod).invoke(instance);
        }
        
    }
}