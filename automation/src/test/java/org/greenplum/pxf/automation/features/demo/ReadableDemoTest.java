package org.greenplum.pxf.automation.features.demo;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.testng.annotations.Test;

public class ReadableDemoTest extends BaseFeature {
    @Override
    public void beforeMethod() throws Exception {
        // default external table with common settings
        exTable = new ReadableExternalTable("demo_table", null, "/tmp/dummy/path", "text");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile("demo");
        exTable.setDelimiter(",");
        exTable.setFields(new String[]{"a text", "b text", "c text"});
        gpdb.createTableAndVerify(exTable);
    }

    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void testDemo() throws Exception {
        runTincTest("pxf.features.demo.readableDemo.runTest");
    }
}
