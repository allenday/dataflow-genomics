package com.google.allenday.genomics.core.main.cmd;

import com.google.allenday.genomics.core.cmd.CmdExecutor;
import com.google.allenday.genomics.core.io.FileUtils;
import org.javatuples.Triplet;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;


public class CmdExecutorTest implements Serializable {

    @Test
    public void testCmdExecutor() throws IOException {
        CmdExecutor cmdExecutor = new CmdExecutor();

        Triplet<Boolean, Integer, String> pwd = cmdExecutor.executeCommand("pwd", false, 3);

        Assert.assertTrue(pwd.getValue0());
        Assert.assertEquals(new Integer(0), pwd.getValue1());
        Assert.assertEquals(new FileUtils().getCurrentPath(), pwd.getValue2() + "/");
    }
}