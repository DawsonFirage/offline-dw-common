package com.dawson.bigdata.offlineDw.constant;

import java.io.File;

public class PathConstant {

    /**
     * Windows 系统本地文件路径前缀
     */
    public static final String DEFAULT_LOCAL_PATH_PREFIX = "file:///";

    /**
     * Windows 系统本地项目路径
     */
    public static final String DEFAULT_LOCAL_PROJECT_ROOT_PATH = DEFAULT_LOCAL_PATH_PREFIX + new File("").getAbsolutePath();

    /**
     * Windows 系统本地项目Source目录路径
     */
    public static final String DEFAULT_LOCAL_SOURCE_PATH = DEFAULT_LOCAL_PROJECT_ROOT_PATH + "/src/main/resources";

    /**
     * Windows 系统本地桌面路径
     */
    public static final String DEFAULT_LOCAL_DESKTOP_PATH = DEFAULT_LOCAL_PATH_PREFIX + "C:/Users/" + System.getenv("USERNAME") + "/Desktop";

    /**
     * Windows 系统数据输出路径
     */
    public static final String DEFAULT_LOCAL_OUTPUT_PATH = DEFAULT_LOCAL_DESKTOP_PATH + "/output";

}
