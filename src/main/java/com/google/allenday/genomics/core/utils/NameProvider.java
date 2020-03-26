package com.google.allenday.genomics.core.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class NameProvider {

    private String startTime;
    private NameProvider(String startTime) {
        this.startTime = startTime;
    }

    public static NameProvider initialize() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd--HH-mm-ss-z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return new NameProvider(simpleDateFormat.format(new Date()));
    }

    public String getCurrentTimeInDefaultFormat() {
        return this.startTime;
    }

    public String buildJobName(String jobNamePrefix, List<String> suffixesList) {
        StringBuilder jobNameBuilder = new StringBuilder(jobNamePrefix);

        if (suffixesList != null && suffixesList.size() > 0) {
            jobNameBuilder.append(String.join("-", suffixesList));
            jobNameBuilder.append("--");
        }
        jobNameBuilder.append(startTime);

        return jobNameBuilder.toString().toLowerCase().replace("_", "-");
    }
}
