package org.greenplum.pxf.automation.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import listeners.CustomAutomationLogger;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.automation.enums.EnumPartitionType;
import org.greenplum.pxf.automation.structures.data.DataPattern;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.hive.HiveTable;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.testng.annotations.Test;

import jsystem.framework.system.SystemManagerImpl;

import org.greenplum.pxf.automation.components.common.DbSystemObject;
import org.greenplum.pxf.automation.components.hive.Hive;
import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.utils.data.DataUtils;
import org.greenplum.pxf.automation.features.BaseFeature;

@Test(groups = "performance")
public class PerformanceTest extends BaseFeature {

    private static final String GENERATE_TEXT_DATA_COL_DELIMITER = ",";
    private static final long GENERATE_TEXT_DATA_SIZE_MB = 16384;
    private static final int GENERATE_COLUMN_MAX_WIDTH = 50;
    private static final int GENERATE_INT_COLUMNS_NUMBER = 5;
    private static final int GENERATE_TEXT_COLUMNS_NUMBER = 5;

    private static final int SAMPLES_NUMBER = 2;

    //Values for filters
    private static final String FILTER_10_PERCENT_RANGE = StringUtils.repeat(
            "F", GENERATE_COLUMN_MAX_WIDTH); // Select 10% range
    private static final String FILTER_1_PERCENT_RANGE = StringUtils.repeat(
            "A", GENERATE_COLUMN_MAX_WIDTH); // Select 1% range

    //Use cases
    private static final String COUNT_WITHOUT_FILTER = "Count total number of rows in table";
    private static final String COUNT_10_PERCENT = "Count number of rows in table 10% range";
    private static final String COUNT_1_PERCENT = "Count number of rows in table 1% range";
    private static final String SELECT_WITHOUT_FILTER_ALL_COLUMNS = "Select all rows, all columns";
    private static final String SELECT_10_PERCENT_ALL_COLUMNS = "Select 10% rows, all columns";
    private static final String SELECT_1_PERCENT_ALL_COLUMNS = "Select 1% rows, all columns";
    private static final String SELECT_WITHOUT_FILTER_ONE_COLUMN = "Select all rows, one column";
    private static final String SELECT_10_PERCENT_ONE_COLUMN = "Select 10% rows, one column";
    private static final String SELECT_1_PERCENT_ONE_COLUMN = "Select 1% rows, one column";

    Hive hive;

    HiveTable hiveTextPerfTable = null;
    HiveTable hiveOrcPerfTable = null;
    HiveTable hiveRcPerfTable = null;
    HiveTable hiveParquetPerfTable = null;
    HiveTable hiveJsonPerfTable = null;

    ReadableExternalTable gpdbTextProfile = null;
    ReadableExternalTable gpdbTextByLineProfile = null;
    ReadableExternalTable gpdbTextMultiProfile = null;
    ReadableExternalTable gpdbTextFileAsRowProfile = null;
    ReadableExternalTable gpdbTextHiveProfile = null;
    ReadableExternalTable gpdbTextHiveTextProfile = null;
    ReadableExternalTable gpdbOrcHiveProfile = null;
    ReadableExternalTable gpdbOrcVectorizedHiveProfile = null;
    ReadableExternalTable gpdbRcHiveProfile = null;
    ReadableExternalTable gpdbParquetProfile = null;
    ReadableExternalTable gpdbJsonProfile = null;
    ReadableExternalTable gpdbJdbcProfile = null;
    ReadableExternalTable gpdbJdbcManyPartitionsProfile = null;
    ReadableExternalTable gpdbJdbcIdealPartitionsProfile = null;

    Table gpdbNativeTable = null;

    List<Table> allTables = null;
    List<Table> noFilterTables = null;

    protected void prepareData() throws Exception {
        hive = (Hive) SystemManagerImpl.getInstance().getSystemObject("hive");
        // setup ownership only if we don't have a kerberized cluster
        if (StringUtils.isEmpty(cluster.getTestKerberosPrincipal())) {
            hdfs.setOwner("/" + hdfs.getWorkingDirectory(), hive.getUserName(),
                    hive.getUserName());
        }

        prepareTextData();
        prepareOrcData();
//        prepareRcData();
        prepareParquetData();
//        prepareJsonData();
        prepareNativeGpdbData();
        prepareJdbcData();
    }

    private void prepareTextData() throws Exception {
        hiveTextPerfTable = TableFactory.getHiveByRowCommaTable(
                "perf_test_text", getColumnTypeHive());
        DataPattern dp = new DataPattern();
        ArrayList<String> columnsTypeList = new ArrayList<String>();

        columnsTypeList.addAll(getColumnTypeDataPattern());

        dp.setCoulmnsTypeList(columnsTypeList);
        dp.setColumnDelimiter(GENERATE_TEXT_DATA_COL_DELIMITER);
        dp.setDataSizeInMegaBytes(GENERATE_TEXT_DATA_SIZE_MB);
        dp.setRandomValues(true);
        dp.setColumnMaxSize(GENERATE_COLUMN_MAX_WIDTH);
        hiveTextPerfTable.setDataPattern(dp);
        hiveTextPerfTable.setStoredAs("TEXTFILE");
        hive.createTableAndVerify(hiveTextPerfTable);

        // Prepare Hdfs data
        long linesNumGenerated = DataUtils.generateAndLoadData(
                hiveTextPerfTable, hdfs);
        String filePath = hdfs.getWorkingDirectory() + "/"
                + hiveTextPerfTable.getName();

        // Prepare Hive table
        hive.loadData(hiveTextPerfTable, filePath, false);

        gpdbTextHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_text_hive_profile", getColumnTypeGpdb(),
                hiveTextPerfTable, true);
        gpdbTextHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbTextHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbTextHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbTextHiveProfile);

        // Prepare Hive text table
        gpdbTextHiveTextProfile = TableFactory.getPxfHiveTextReadableTable(
                "perf_text_hive_text_profile", getColumnTypeGpdb(),
                hiveTextPerfTable, true);
        gpdbTextHiveTextProfile.setProfile(EnumPxfDefaultProfiles.HiveText
                .toString());
        gpdbTextHiveTextProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbTextHiveTextProfile.setPort(pxfPort);
        gpdbTextHiveTextProfile.setDelimiter(",");
        gpdb.createTableAndVerify(gpdbTextHiveTextProfile);

        // Prepare hdfs simple text table
        gpdbTextProfile = new ReadableExternalTable("perf_text_profile", getColumnTypeGpdb(),
                hiveTextPerfTable.getlocation(), "TEXT");
        gpdbTextProfile.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        gpdbTextProfile.setDelimiter(",");
        gpdbTextProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbTextProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbTextProfile);

        // Prepare hdfs simple text table using linereader
        gpdbTextByLineProfile = new ReadableExternalTable("perf_textline_profile", getColumnTypeGpdb(),
                hiveTextPerfTable.getlocation(), "TEXT");
        gpdbTextByLineProfile.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        gpdbTextByLineProfile.setDelimiter(",");
        gpdbTextByLineProfile.setUserParameters(new String[]{"LINEREADER=true"});
        gpdbTextByLineProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbTextByLineProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbTextByLineProfile);

//        // Prepare hdfs multi text table
//        gpdbTextMultiProfile = new ReadableExternalTable("perf_textmulti_profile", getColumnTypeGpdb(),
//                hiveTextPerfTable.getlocation(), "TEXT");
//        gpdbTextMultiProfile.setProfile(EnumPxfDefaultProfiles.HdfsTextMulti.toString());
//        gpdbTextMultiProfile.setDelimiter(",");
//        gpdbTextMultiProfile.setHost(/* pxfHost */"127.0.0.1");
//        gpdbTextMultiProfile.setPort(pxfPort);
//        gpdb.createTableAndVerify(gpdbTextMultiProfile);

//        // Prepare hdfs multi text table with FILE_AS_ROW
//        gpdbTextFileAsRowProfile = new ReadableExternalTable("perf_text_fileasrow_profile", new String[]{"data text"},
//                hiveTextPerfTable.getlocation(), "CSV");
//        gpdbTextFileAsRowProfile.setProfile(EnumPxfDefaultProfiles.HdfsTextMulti.toString());
//        gpdbTextFileAsRowProfile.setDelimiter(",");
//        gpdbTextFileAsRowProfile.setUserParameters(new String[]{"FILE_AS_ROW=true"});
//        gpdbTextFileAsRowProfile.setHost(/* pxfHost */"127.0.0.1");
//        gpdbTextFileAsRowProfile.setPort(pxfPort);
//        gpdb.createTableAndVerify(gpdbTextFileAsRowProfile);

    }

    private void prepareOrcData() throws Exception {

        hiveOrcPerfTable = TableFactory.getHiveByRowCommaTable("perf_test_orc",
                getColumnTypeHive());
        hiveOrcPerfTable.setStoredAs("ORC");
        hive.createTableAndVerify(hiveOrcPerfTable);

        hive.insertData(hiveTextPerfTable, hiveOrcPerfTable);

        gpdbOrcHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_orc_hive_profile", getColumnTypeGpdb(), hiveOrcPerfTable,
                true);

        gpdbOrcHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbOrcHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbOrcHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbOrcHiveProfile);

        gpdbOrcVectorizedHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_orc_vectorized_hive_profile", getColumnTypeGpdb(), hiveOrcPerfTable,
                true);

        gpdbOrcVectorizedHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbOrcVectorizedHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbOrcVectorizedHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbOrcVectorizedHiveProfile);

    }

    private void prepareRcData() throws Exception {
        hiveRcPerfTable = TableFactory.getHiveByRowCommaTable("perf_test_rc",
                getColumnTypeHive());
        hiveRcPerfTable.setStoredAs("RCFILE");
        hive.createTableAndVerify(hiveRcPerfTable);

        hive.insertData(hiveTextPerfTable, hiveRcPerfTable);

        gpdbRcHiveProfile = TableFactory.getPxfHiveReadableTable(
                "perf_rc_hive_profile", getColumnTypeGpdb(), hiveRcPerfTable,
                true);

        gpdbRcHiveProfile.setProfile(EnumPxfDefaultProfiles.Hive.toString());
        gpdbRcHiveProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbRcHiveProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbRcHiveProfile);
    }

    private void prepareParquetData() throws Exception {
        hiveParquetPerfTable = TableFactory.getHiveByRowCommaTable("perf_test_parquet",
                getColumnTypeHive());
        hiveParquetPerfTable.setStoredAs("PARQUET");
        hive.createTableAndVerify(hiveParquetPerfTable);

        hive.insertData(hiveTextPerfTable, hiveParquetPerfTable);

        gpdbParquetProfile = TableFactory.getPxfParquetReadableTable(
                "perf_parquet_profile", getColumnTypeGpdb(), hiveParquetPerfTable.getlocation());
        gpdbParquetProfile.setProfile(EnumPxfDefaultProfiles.Parquet.toString());
        gpdbParquetProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbParquetProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbParquetProfile);
    }

    private void prepareJsonData() throws Exception {
        hiveJsonPerfTable = new HiveTable("perf_test_json", getColumnTypeHive());
        hiveJsonPerfTable.setFormat("ROW");
        hiveJsonPerfTable.setSerde("org.apache.hive.hcatalog.data.JsonSerDe");
        hiveJsonPerfTable.setStoredAs("TEXTFILE");
        hive.createTableAndVerify(hiveJsonPerfTable);

        hive.insertData(hiveTextPerfTable, hiveJsonPerfTable);

        gpdbJsonProfile = TableFactory.getPxfJsonReadableTable(
                "perf_json_profile", getColumnTypeGpdb(), hiveJsonPerfTable.getlocation());
        gpdbJsonProfile.setProfile(EnumPxfDefaultProfiles.JSON.toString());
        gpdbJsonProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbJsonProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbJsonProfile);
    }

    private void prepareJdbcData() throws Exception {
        gpdbJdbcProfile = TableFactory.getPxfJdbcReadableTable(
                "perf_jdbc_profile",
                getColumnTypeGpdb(),
                gpdbNativeTable.getName(),
                "org.postgresql.Driver",
                "jdbc:postgresql://"+ gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                gpdb.getUserName());
        gpdbJdbcProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbJdbcProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbJdbcProfile);

        gpdbJdbcManyPartitionsProfile = TableFactory.getPxfJdbcReadablePartitionedTable(
                "perf_jdbc_manypartitions_profile",
                getColumnTypeGpdb(),
                gpdbNativeTable.getName(),
                "org.postgresql.Driver",
                "jdbc:postgresql://"+ gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                0, "-2147480575:2147482993", "100000000",
                gpdb.getUserName(), EnumPartitionType.INT, null);
        gpdbJdbcManyPartitionsProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbJdbcManyPartitionsProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbJdbcManyPartitionsProfile);

        gpdbJdbcIdealPartitionsProfile = TableFactory.getPxfJdbcReadablePartitionedTable(
                "perf_jdbc_idealpartitions_profile",
                getColumnTypeGpdb(),
                gpdbNativeTable.getName(),
                "org.postgresql.Driver",
                "jdbc:postgresql://"+ gpdb.getMasterHost() + ":" + gpdb.getPort() + "/pxfautomation",
                0, "-2147480575:2147482993", "1435000000",
                gpdb.getUserName(), EnumPartitionType.INT, null);
        gpdbJdbcIdealPartitionsProfile.setHost(/* pxfHost */"127.0.0.1");
        gpdbJdbcIdealPartitionsProfile.setPort(pxfPort);
        gpdb.createTableAndVerify(gpdbJdbcIdealPartitionsProfile);
    }

    private void prepareNativeGpdbData() throws Exception {
        gpdbNativeTable = new Table("perf_test", getColumnTypeGpdb());
        gpdbNativeTable.setDistributionFields(new String[]{"int0"});
        gpdb.createTableAndVerify(gpdbNativeTable);

        gpdb.copyData(gpdbTextHiveProfile, gpdbNativeTable);
    }

    @Override
    protected void beforeClass() throws Exception {
        prepareData();
        allTables = new ArrayList<>();
        allTables.add(gpdbNativeTable);
        allTables.add(gpdbTextProfile);
        allTables.add(gpdbTextByLineProfile);
//        allTables.add(gpdbTextMultiProfile);
        allTables.add(gpdbTextHiveProfile);
        allTables.add(gpdbTextHiveTextProfile);
        allTables.add(gpdbOrcHiveProfile);
        allTables.add(gpdbOrcVectorizedHiveProfile);
//        allTables.add(gpdbRcHiveProfile);
        allTables.add(gpdbParquetProfile);
//        allTables.add(gpdbJsonProfile);
        allTables.add(gpdbJdbcProfile);
        allTables.add(gpdbJdbcManyPartitionsProfile);
        allTables.add(gpdbJdbcIdealPartitionsProfile);

//        noFilterTables = new ArrayList<>();
//        noFilterTables.add(gpdbTextFileAsRowProfile);

        CustomAutomationLogger.revertStdoutStream();
        printPerformanceReport();
    }

    @Test(groups = "performance")
    public void testCountWithoutFilter() throws Exception {

//        runAndReportQueries("SELECT COUNT(*) FROM %s", COUNT_WITHOUT_FILTER,
//                Stream.concat(allTables.stream(), noFilterTables.stream()).collect(Collectors.toList()));
        runAndReportQueries("SELECT COUNT(*) FROM %s", COUNT_WITHOUT_FILTER, allTables);
    }

    @Test(groups = "performance")
    public void testCount1PercentRange() throws Exception {

        runAndReportQueries("SELECT COUNT(*) FROM %s WHERE str0 < '"
                + FILTER_1_PERCENT_RANGE + "'", COUNT_1_PERCENT, allTables);
    }

    @Test(groups = "performance", enabled = false)
    public void testCount10PercentRange() throws Exception {

        runAndReportQueries("SELECT COUNT(*) FROM %s WHERE str0 < '"
                + FILTER_10_PERCENT_RANGE + "'", COUNT_10_PERCENT, allTables);
    }

    @Test(groups = "performance")
    public void testSelectAllRowsAllColumns() throws Exception {

//        runAndReportQueries("SELECT * FROM %s", SELECT_WITHOUT_FILTER_ALL_COLUMNS,
//                Stream.concat(allTables.stream(), noFilterTables.stream()).collect(Collectors.toList()));
        runAndReportQueries("SELECT * FROM %s", SELECT_WITHOUT_FILTER_ALL_COLUMNS, allTables);
    }

    @Test(groups = "performance")
    public void testSelect1PercentRowsAllColumns() throws Exception {

        runAndReportQueries("SELECT * FROM %s WHERE str0 < '"
                        + FILTER_1_PERCENT_RANGE + "'", SELECT_1_PERCENT_ALL_COLUMNS,
                allTables);
    }

    @Test(groups = "performance", enabled = false)
    public void testSelect10PercentRowsAllColumns() throws Exception {

        runAndReportQueries("SELECT * FROM %s WHERE str0 < '"
                + FILTER_10_PERCENT_RANGE + "'", SELECT_10_PERCENT_ALL_COLUMNS,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelect1PercentRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s WHERE str0 < '"
                        + FILTER_1_PERCENT_RANGE + "'", SELECT_1_PERCENT_ONE_COLUMN,
                allTables);
    }

    @Test(groups = "performance", enabled = false)
    public void testSelect10PercentRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s WHERE str0 < '"
                + FILTER_10_PERCENT_RANGE + "'", SELECT_10_PERCENT_ONE_COLUMN,
                allTables);
    }

    @Test(groups = "performance")
    public void testSelectAllRowsOneColumn() throws Exception {

        runAndReportQueries("SELECT str0 FROM %s",
                SELECT_WITHOUT_FILTER_ONE_COLUMN, allTables);
    }

    private void runAndReportQueries(String queryTemplate, String queryType,
            List<Table> tables) throws Exception {

        Map<Long, Table> results = new LinkedHashMap<Long, Table>();

        for (Table table : tables) {
            String query = String.format(queryTemplate, table.getName());
            Long avgTime = measureAverageQueryTime(query, getDbForTable(table));
            results.put(avgTime, table);
        }

        CustomAutomationLogger.revertStdoutStream();
        System.out.println("\n\n======================================");
        System.out.println(queryType);
        System.out.println("======================================");
        for (Entry<Long, Table> entry : results.entrySet()) {
            String query = String.format(queryTemplate, entry.getValue()
                    .getName());
            printPerformanceReportPerTable(queryType, query, entry.getValue(),
                    entry.getKey());
        }
    }

    private List<String> getColumnTypeDataPattern() {
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < GENERATE_INT_COLUMNS_NUMBER; i++)
            result.add("INTEGER");

        for (int i = 0; i < GENERATE_TEXT_COLUMNS_NUMBER; i++)
            result.add("TEXT");

        return result;
    }

    private String[] getColumnTypeGpdb() {
        String[] result = new String[GENERATE_INT_COLUMNS_NUMBER
                + GENERATE_TEXT_COLUMNS_NUMBER];

        for (int i = 0; i < GENERATE_INT_COLUMNS_NUMBER; i++)
            result[i] = "int" + i + " int4";

        for (int i = 0; i < GENERATE_TEXT_COLUMNS_NUMBER; i++)
            result[i + GENERATE_INT_COLUMNS_NUMBER] = "str" + i + " text";

        return result;

    }

    private String[] getColumnTypeHive() {
        String[] result = new String[GENERATE_INT_COLUMNS_NUMBER
                + GENERATE_TEXT_COLUMNS_NUMBER];

        for (int i = 0; i < GENERATE_INT_COLUMNS_NUMBER; i++)
            result[i] = "int" + i + " int";

        for (int i = 0; i < GENERATE_TEXT_COLUMNS_NUMBER; i++)
            result[i + GENERATE_INT_COLUMNS_NUMBER] = "str" + i + " string";

        return result;

    }

    private DbSystemObject getDbForTable(Table table) throws Exception {
        if (table instanceof HiveTable)
            return hive;
        else if (table instanceof ReadableExternalTable
                || table instanceof Table)
            return gpdb;
        else
            throw new Exception("Unable to get db engine for table: "
                    + table.getClass());
    }

    private String getTableInfo(Table table) throws Exception {
        if (table instanceof HiveTable)
            return "Hive table stored as " + ((HiveTable) table).getStoredAs();
        else if (table instanceof ReadableExternalTable)
            return "External Gpdb table " + table.getName() + ", with profile "
                    + ((ReadableExternalTable) table).getProfile();
        else if (table instanceof Table)
            return "Native Gpdb table";
        else
            throw new Exception(
                    "Unable to print table details, unknown table: "
                            + table.getClass());
    }

    private void printPerformanceReportPerTable(String queryType, String query,
            Table table, long avgTime) throws Exception {
        DbSystemObject db = getDbForTable(table);
        String tableInfo = getTableInfo(table);
        System.out.println("\nTable Info: " + tableInfo);
        System.out.println("AVERAGE TIME: " + avgTime + " MILLISECONDS");
    }

    private void printPerformanceReport() {
        System.out.println("\n\n########################");
        System.out.println("PERFORMANCE RESULTS");
        System.out.println("########################");
        System.out.println("Initial data size in text format: " + GENERATE_TEXT_DATA_SIZE_MB + " Mb");
        System.out.println("String column width: " + GENERATE_COLUMN_MAX_WIDTH);
        System.out.println("Number of samples per each query: " + SAMPLES_NUMBER);
    }

    private long measureAverageQueryTime(String query, DbSystemObject db)
            throws Exception {
        long avgQueryTime = 0;
        for (int i = 0; i < SAMPLES_NUMBER; i++)
            avgQueryTime += db.runQueryTiming(query);

        avgQueryTime /= SAMPLES_NUMBER;
        return avgQueryTime;
    }
}
