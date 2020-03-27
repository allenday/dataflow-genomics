package com.google.allenday.genomics.core.io;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IoUtils implements Serializable {

    public String getStringContentFromByteBuffer(ByteBuffer byteBuffer) {
        return StandardCharsets.UTF_8.decode(byteBuffer).toString();
    }
}
