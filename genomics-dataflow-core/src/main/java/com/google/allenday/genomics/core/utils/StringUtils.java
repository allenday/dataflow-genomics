package com.google.allenday.genomics.core.utils;

public class StringUtils {

    public static String generateSlug(String str){
        return str.toLowerCase()
                .replace(" ", "_")
                .replace(".","_")
                .replace("-","_");
    }
}
