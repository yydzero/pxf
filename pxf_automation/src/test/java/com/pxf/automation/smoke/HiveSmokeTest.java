package com.pxf.automation.smoke;

import jsystem.framework.system.SystemManagerImpl;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import com.pxf.automation.components.hive.Hive;
import com.pxf.automation.structures.tables.basic.Table;
import com.pxf.automation.structures.tables.hive.HiveTable;
import com.pxf.automation.structures.tables.utils.TableFactory;

/** Basic PXF on small Hive table */
public class HiveSmokeTest extends BaseSmoke {
    Hive hive;
    Table dataTable;
    HiveTable hiveTable;
    String fileName = "hiveSmallData.txt";

    @Override
    public void beforeClass() throws Exception {
        // Initialize Hive system object
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
    }

    @Override
    public void afterClass() throws Exception {
        // close hive connection
        if (hive != null)
            hive.close();
    }

    @Override
    protected void prepareData() throws Exception {
        // Create Hive table
        hiveTable = TableFactory.getHiveByRowCommaTable("hive_table", new String[] {
                "s1 string",
                "n1 int",
                "d1 double",
                "bg bigint",
                "b boolean"
        });
        // hive.dropTable(hiveTable, false);
        hive.createTableAndVerify(hiveTable);
        // Generate Small data, write to HDFS and load to Hive
        Table dataTable = getSmallData();
        hdfs.writeTableToFile((hdfs.getWorkingDirectory() + "/" + fileName), dataTable, ",");

        // load data from HDFS file
        hive.loadData(hiveTable, (hdfs.getWorkingDirectory() + "/" + fileName), false);
    }

    @Override
    protected void createTables() throws Exception {
        // Create HAWQ external table
        exTable = TableFactory.getPxfHiveReadableTable("pxf_smoke_small_data", new String[] {
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hiveTable, true);
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        hawq.createTableAndVerify(exTable);
    }

    @Override
    protected void queryResults() throws Exception {
        runTincTest("pxf.smoke.small_data.runTest");
        runTincTest("pxf.smoke.hcatalog_small_data.runTest");
    }

    @Test(groups = { "smoke", "gpdb" })
    public void test() throws Exception {
        runTest();
    }
}