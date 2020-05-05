package com.google.allenday.genomics.core.worker.cmd;

public class Commands {

    public final static String CMD_APT_UPDATE = "apt-get update";
    public final static String CMD_APT_GET_INSTALL_FORMAT = "apt-get install %s -y";

    public final static String CMD_INSTALL_UNZIP = String.format(CMD_APT_GET_INSTALL_FORMAT, "unzip");
    public final static String CMD_INSTALL_BZIP2 = String.format(CMD_APT_GET_INSTALL_FORMAT, "bzip2");
    public final static String CMD_INSTALL_CURL = String.format(CMD_APT_GET_INSTALL_FORMAT, "curl");
    public final static String CMD_INSTALL_WGET = String.format(CMD_APT_GET_INSTALL_FORMAT, "wget");
    public final static String CMD_INSTALL_LIB_XML2_DEV = String.format(CMD_APT_GET_INSTALL_FORMAT, "libxml2-dev");
    public final static String CMD_INSTALL_ZLIB1G_DEV = String.format(CMD_APT_GET_INSTALL_FORMAT, "zlib1g-dev");
    public final static String CMD_INSTALL_BUILD_ESSENTIALS = String.format(CMD_APT_GET_INSTALL_FORMAT, "build-essential");
    public final static String CMD_INSTALL_PYTHON_2_7 = String.format(CMD_APT_GET_INSTALL_FORMAT, "python");

    private final static String CMD_WGET_FORMAT = "wget %s";
    private final static String CMD_SH_FORMAT = "sh %s";
    private final static String CMD_UNZIP_FORMAT = "unzip -o %s";
    private final static String CMD_RM_FORMAT = "rm -f %s";
    private final static String CMD_CD_AND_MAKE_FORMAT = "cd %s; make";
    private final static String CMD_UNTAR_FORMAT = "tar -xvjf %s -C %s";

    public static String wget(String name) {
        return String.format(CMD_WGET_FORMAT, name);
    }

    public static String sh(String name) {
        return String.format(CMD_SH_FORMAT, name);
    }

    public static String unzip(String name) {
        return String.format(CMD_UNZIP_FORMAT, name);
    }

    public static String rm(String name) {
        return String.format(CMD_RM_FORMAT, name);
    }

    public static String cdAndMake(String path) {
        return String.format(CMD_CD_AND_MAKE_FORMAT, path);
    }

    public static String untar(String file, String dest) {
        return String.format(CMD_UNTAR_FORMAT, file, dest);
    }
}
