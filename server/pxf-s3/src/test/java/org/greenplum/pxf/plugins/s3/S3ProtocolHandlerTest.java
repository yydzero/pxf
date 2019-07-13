package org.greenplum.pxf.plugins.s3;

import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class S3ProtocolHandlerTest {

    private static final String FILE_FRAGMENTER = "org.greenplum.pxf.plugins.hdfs.HdfsFileFragmenter";
    private static final String STRING_PASS_RESOLVER = "org.greenplum.pxf.plugins.hdfs.StringPassResolver";
    private static final String S3_ACCESSOR = S3SelectAccessor.class.getName();
    private static final String DEFAULT_ACCESSOR = "default-accessor";
    private static final String DEFAULT_RESOLVER = "default-resolver";
    private static final String DEFAULT_FRAGMENTER = "default-fragmenter";
    private static final String NOT_SUPPORTED = "ERROR";

    private static Map<String, String> EXPECTED_RESOLVER_TEXT_ON;
    private static Map<String, String> EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT;
    private static Map<String, String> EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT;
    private static Map<String, String> EXPECTED_RESOLVER_TEXT_OFF;
    private static Map<String, String> EXPECTED_RESOLVER_GPDB_WRITABLE_ON;
    private static Map<String, String> EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO;
    private static Map<String, String> EXPECTED_RESOLVER_GPDB_WRITABLE_OFF;
    private static Map<String, String> EXPECTED_FRAGMENTER_TEXT_ON;
    private static Map<String, String> EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT;
    private static Map<String, String> EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT;
    private static Map<String, String> EXPECTED_FRAGMENTER_TEXT_OFF;
    private static Map<String, String> EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON;
    private static Map<String, String> EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO;
    private static Map<String, String> EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF;
    private static Map<String, String> EXPECTED_ACCESSOR_TEXT_ON;
    private static Map<String, String> EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT;
    private static Map<String, String> EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT;
    private static Map<String, String> EXPECTED_ACCESSOR_TEXT_OFF;
    private static Map<String, String> EXPECTED_ACCESSOR_GPDB_WRITABLE_ON;
    private static Map<String, String> EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO;
    private static Map<String, String> EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF;

    static {
        EXPECTED_RESOLVER_TEXT_ON = new HashMap<>();
        EXPECTED_RESOLVER_TEXT_ON.put("text", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_ON.put("csv", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_ON.put("json", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_ON.put("parquet", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_ON.put("avro", NOT_SUPPORTED);

        EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT = new HashMap<>();
        EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT.put("text", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT.put("csv", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT.put("json", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT.put("parquet", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT.put("avro", DEFAULT_RESOLVER);

        EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT = new HashMap<>();
        EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT.put("text", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT.put("csv", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT.put("json", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT.put("parquet", STRING_PASS_RESOLVER);
        EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT.put("avro", DEFAULT_RESOLVER);

        EXPECTED_RESOLVER_TEXT_OFF = new HashMap<>();
        EXPECTED_RESOLVER_TEXT_OFF.put("text", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_TEXT_OFF.put("csv", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_TEXT_OFF.put("json", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_TEXT_OFF.put("parquet", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_TEXT_OFF.put("avro", DEFAULT_RESOLVER);

        EXPECTED_RESOLVER_GPDB_WRITABLE_ON = new HashMap<>();
        EXPECTED_RESOLVER_GPDB_WRITABLE_ON.put("text", NOT_SUPPORTED);
        EXPECTED_RESOLVER_GPDB_WRITABLE_ON.put("csv", NOT_SUPPORTED);
        EXPECTED_RESOLVER_GPDB_WRITABLE_ON.put("json", NOT_SUPPORTED);
        EXPECTED_RESOLVER_GPDB_WRITABLE_ON.put("parquet", NOT_SUPPORTED);
        EXPECTED_RESOLVER_GPDB_WRITABLE_ON.put("avro", NOT_SUPPORTED);

        EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO = new HashMap<>();
        EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO.put("text", NOT_SUPPORTED);
        EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO.put("csv", NOT_SUPPORTED);
        EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO.put("json", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO.put("parquet", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO.put("avro", DEFAULT_RESOLVER);

        EXPECTED_RESOLVER_GPDB_WRITABLE_OFF = new HashMap<>();
        EXPECTED_RESOLVER_GPDB_WRITABLE_OFF.put("text", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_GPDB_WRITABLE_OFF.put("csv", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_GPDB_WRITABLE_OFF.put("json", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_GPDB_WRITABLE_OFF.put("parquet", DEFAULT_RESOLVER);
        EXPECTED_RESOLVER_GPDB_WRITABLE_OFF.put("avro", DEFAULT_RESOLVER);

        EXPECTED_FRAGMENTER_TEXT_ON = new HashMap<>();
        EXPECTED_FRAGMENTER_TEXT_ON.put("text", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_ON.put("csv", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_ON.put("json", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_ON.put("parquet", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_ON.put("avro", NOT_SUPPORTED);

        EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT = new HashMap<>();
        EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT.put("text", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT.put("csv", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT.put("json", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT.put("parquet", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT.put("avro", DEFAULT_FRAGMENTER);

        EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT = new HashMap<>();
        EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT.put("text", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT.put("csv", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT.put("json", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT.put("parquet", FILE_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT.put("avro", DEFAULT_FRAGMENTER);

        EXPECTED_FRAGMENTER_TEXT_OFF = new HashMap<>();
        EXPECTED_FRAGMENTER_TEXT_OFF.put("text", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_OFF.put("csv", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_OFF.put("json", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_OFF.put("parquet", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_TEXT_OFF.put("avro", DEFAULT_FRAGMENTER);

        EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON = new HashMap<>();
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON.put("text", NOT_SUPPORTED);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON.put("csv", NOT_SUPPORTED);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON.put("json", NOT_SUPPORTED);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON.put("parquet", NOT_SUPPORTED);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON.put("avro", NOT_SUPPORTED);

        EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO = new HashMap<>();
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO.put("text", NOT_SUPPORTED);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO.put("csv", NOT_SUPPORTED);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO.put("json", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO.put("parquet", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO.put("avro", DEFAULT_FRAGMENTER);

        EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF = new HashMap<>();
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF.put("text", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF.put("csv", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF.put("json", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF.put("parquet", DEFAULT_FRAGMENTER);
        EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF.put("avro", DEFAULT_FRAGMENTER);

        EXPECTED_ACCESSOR_TEXT_ON = new HashMap<>();
        EXPECTED_ACCESSOR_TEXT_ON.put("text", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_ON.put("csv", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_ON.put("json", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_ON.put("parquet", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_ON.put("avro", NOT_SUPPORTED);

        EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT = new HashMap<>();
        EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT.put("text", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT.put("csv", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT.put("json", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT.put("parquet", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT.put("avro", DEFAULT_ACCESSOR);

        EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT = new HashMap<>();
        EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT.put("text", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT.put("csv", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT.put("json", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT.put("parquet", S3_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT.put("avro", DEFAULT_ACCESSOR);

        EXPECTED_ACCESSOR_TEXT_OFF = new HashMap<>();
        EXPECTED_ACCESSOR_TEXT_OFF.put("text", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_OFF.put("csv", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_OFF.put("json", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_OFF.put("parquet", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_TEXT_OFF.put("avro", DEFAULT_ACCESSOR);

        EXPECTED_ACCESSOR_GPDB_WRITABLE_ON = new HashMap<>();
        EXPECTED_ACCESSOR_GPDB_WRITABLE_ON.put("text", NOT_SUPPORTED);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_ON.put("csv", NOT_SUPPORTED);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_ON.put("json", NOT_SUPPORTED);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_ON.put("parquet", NOT_SUPPORTED);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_ON.put("avro", NOT_SUPPORTED);

        EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO = new HashMap<>();
        EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO.put("text", NOT_SUPPORTED);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO.put("csv", NOT_SUPPORTED);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO.put("json", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO.put("parquet", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO.put("avro", DEFAULT_ACCESSOR);

        EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF = new HashMap<>();
        EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF.put("text", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF.put("csv", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF.put("json", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF.put("parquet", DEFAULT_ACCESSOR);
        EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF.put("avro", DEFAULT_ACCESSOR);
    }

    private S3ProtocolHandler handler;
    private RequestContext context;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        handler = new S3ProtocolHandler();
        context = new RequestContext();
        context.setFragmenter("default-fragmenter");
        context.setAccessor("default-accessor");
        context.setResolver("default-resolver");
        List<ColumnDescriptor> columns = new ArrayList<>();
        columns.add(new ColumnDescriptor("c1", 1, 0, "INT", null, true)); // actual args do not matter
        columns.add(new ColumnDescriptor("c2", 2, 0, "INT", null, true)); // actual args do not matter
        context.setTupleDescription(columns);
    }

    @Test
    public void testTextWithSelectOn() {
        context.addOption("S3-SELECT", "on");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_ON);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterOnly() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitProjectionOnly() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterAndProjection() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithBenefitFilterAndFullProjection() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setFilterString("abc");
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefit() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithDelimiterOption() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("DELIMITER", "|");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithQuoteCharacterOption() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("QUOTE", "'");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithQuoteEscapeCharacterOption() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("ESCAPE", "\\");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithRecordDelimiterOption() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("NEWLINE", "\r");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithFileHeaderInfoOptionUSE() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("HEADER", "USE");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithFileHeaderInfoOptionIGNORE() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("HEADER", "IGNORE");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitWithFileHeaderInfoOptionNONE() {
        context.addOption("S3-SELECT", "auto");
        context.addOption("HEADER", "NONE");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectAutoWithNoBenefitAllProjected() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.TEXT);
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_AUTO_NO_BENEFIT);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_AUTO_NO_BENEFIT);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_AUTO_NO_BENEFIT);
    }

    @Test
    public void testTextWithSelectOff() {
        context.addOption("S3-SELECT", "off");
        context.setOutputFormat(OutputFormat.TEXT);
        verifyAccessors(context, EXPECTED_ACCESSOR_TEXT_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_TEXT_OFF);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_TEXT_OFF);
    }

    @Test
    public void testGPDBWritableWithSelectOn() {
        context.addOption("S3-SELECT", "on");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_ON);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_ON);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_ON);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterOnly() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitProjectionOnly() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterAndProjection() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        context.setNumAttrsProjected(1);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithBenefitFilterAndFullProjection() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setFilterString("abc");
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithNoBenefit() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectAutoWithNoBenefitAllProjected() {
        context.addOption("S3-SELECT", "auto");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        context.setNumAttrsProjected(2);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_AUTO);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_AUTO);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_AUTO);
    }

    @Test
    public void testGPDBWritableWithSelectOff() {
        context.addOption("S3-SELECT", "off");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        verifyAccessors(context, EXPECTED_ACCESSOR_GPDB_WRITABLE_OFF);
        verifyResolvers(context, EXPECTED_RESOLVER_GPDB_WRITABLE_OFF);
        verifyFragmenters(context, EXPECTED_FRAGMENTER_GPDB_WRITABLE_OFF);
    }

    @Test
    public void testTextSelectOptionMissing() {
        context.setFormat("CSV");
        context.setOutputFormat(OutputFormat.TEXT);
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    @Test
    public void testGPDBWritableSelectOptionMissing() {
        context.setFormat("CSV");
        context.setOutputFormat(OutputFormat.GPDBWritable);
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    @Test
    public void testSelectInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid value 'foo' for S3-SELECT option");

        context.setFormat("CSV");
        context.addOption("S3-SELECT", "foo");
        handler.getAccessorClassName(context);
    }

    @Test
    public void testSelectOffMissingFormat() {
        context.addOption("S3-SELECT", "off");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    @Test
    public void testSelectOffUnsupportedFormat() {
        context.addOption("S3-SELECT", "off");
        context.setFormat("custom");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    @Test
    public void testSelectOnMissingFormat() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("S3-SELECT optimization is not supported for format 'null'");

        context.addOption("S3-SeLeCt", "on");
        handler.getAccessorClassName(context);
    }

    @Test
    public void testSelectOnUnsupportedFormat() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("S3-SELECT optimization is not supported for format 'CUSTOM'");

        context.addOption("S3-SELECT", "on");
        context.setFormat("custom");
        handler.getAccessorClassName(context);
    }

    @Test
    public void testSelectAutoMissingFormat() {
        context.addOption("S3-SELECT", "AUTO");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    @Test
    public void testSelectAutoUnsupportedFormat() {
        context.addOption("S3-SELECT", "Auto");
        context.setFormat("custom");
        assertEquals("default-accessor", handler.getAccessorClassName(context));
        assertEquals("default-resolver", handler.getResolverClassName(context));
        assertEquals("default-fragmenter", handler.getFragmenterClassName(context));
    }

    private void verifyFragmenters(RequestContext context, Map<String, String> expected) {
        for (String format : expected.keySet()) {
            context.setFormat(format);
            try {
                assertEquals(expected.get(format), handler.getFragmenterClassName(context));
            } catch (IllegalArgumentException e) {
                if (!expected.get(format).equals(NOT_SUPPORTED)) {
                    throw e;
                }
            }
        }
    }

    private void verifyResolvers(RequestContext context, Map<String, String> expected) {
        for (String format : expected.keySet()) {
            context.setFormat(format);
            try {
                assertEquals(expected.get(format), handler.getResolverClassName(context));
            } catch (IllegalArgumentException e) {
                if (!expected.get(format).equals(NOT_SUPPORTED)) {
                    throw e;
                }
            }
        }
    }

    private void verifyAccessors(RequestContext context, Map<String, String> expected) {
        for (String format : expected.keySet()) {
            context.setFormat(format);
            try {
                assertEquals(expected.get(format), handler.getAccessorClassName(context));
            } catch (IllegalArgumentException e) {
                if (!expected.get(format).equals(NOT_SUPPORTED)) {
                    throw e;
                }
            }
        }
    }
}
