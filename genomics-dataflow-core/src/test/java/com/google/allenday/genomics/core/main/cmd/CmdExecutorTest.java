package com.google.allenday.genomics.core.main.cmd;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.utils.ValueIterableToValueListTransform;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.javatuples.Triplet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class CmdExecutorTest implements Serializable {

    @Test
    public void testCmdExecutor() throws IOException {
        CmdExecutor cmdExecutor = new CmdExecutor();

        Triplet<Boolean, Integer, String> pwd = cmdExecutor.executeCommand("pwd", false);

        Assert.assertTrue(pwd.getValue0());
        Assert.assertEquals(new Integer(0), pwd.getValue1());
        Assert.assertEquals(new FileUtils().getCurrentPath(), pwd.getValue2()+"/");
    }
}