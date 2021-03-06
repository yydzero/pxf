package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveAccessor.class, HiveUtilities.class, HdfsUtilities.class, HiveDataFragmenter.class})
public class HiveAccessorTest {

    @Mock
    RequestContext requestContext;
    @Mock
    InputFormat inputFormat;
    @Mock
    RecordReader<Object, Object> reader;

    HiveAccessor accessor;
    HiveUserDataBuilder userDataBuilder;

    @Before
    public void setup() throws Exception {
        userDataBuilder = new HiveUserDataBuilder()
                .withSerdeClassName("org.apache.hadoop.mapred.TextInputFormat")
                .withPartitionKeys(HiveDataFragmenter.HIVE_NO_PART_TBL);

        PowerMockito.mockStatic(HiveUtilities.class);
        PowerMockito.mockStatic(HdfsUtilities.class);

        PowerMockito.mockStatic(HiveDataFragmenter.class);

        when(inputFormat.getRecordReader(any(InputSplit.class), any(JobConf.class), any(Reporter.class))).thenReturn(reader);
        PowerMockito.when(requestContext.getAccessor()).thenReturn(HiveORCAccessor.class.getName());
        PowerMockito.when(requestContext.getConfig()).thenReturn("default");
        PowerMockito.when(requestContext.getUser()).thenReturn("test-user");

        @SuppressWarnings("unchecked")
        OngoingStubbing ongoingStubbing = when(HiveDataFragmenter.makeInputFormat(any(String.class), any(JobConf.class))).thenReturn(inputFormat);
    }

    @Test
    public void testSkipHeaderCountGreaterThanZero() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(2).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(requestContext)).thenReturn(userData);
        when(requestContext.hasFilter()).thenReturn(false);

        accessor = new HiveAccessor();
        accessor.initialize(requestContext);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(2).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(requestContext)).thenReturn(userData);
        when(requestContext.hasFilter()).thenReturn(false);
        when(requestContext.getFragmentIndex()).thenReturn(0);

        accessor = new HiveAccessor();
        accessor.initialize(requestContext);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(3)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountGreaterThanZeroNotFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(2).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(requestContext)).thenReturn(userData);
        when(requestContext.hasFilter()).thenReturn(false);
        when(requestContext.getFragmentIndex()).thenReturn(2);

        accessor = new HiveAccessor();
        accessor.initialize(requestContext);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountZeroFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(0).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(requestContext)).thenReturn(userData);
        when(requestContext.hasFilter()).thenReturn(false);
        when(requestContext.getFragmentIndex()).thenReturn(0);

        accessor = new HiveAccessor();
        accessor.initialize(requestContext);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }

    @Test
    public void testSkipHeaderCountNegativeFirstFragment() throws Exception {
        HiveUserData userData = userDataBuilder.withSkipHeader(-1).build();
        PowerMockito.when(HiveUtilities.parseHiveUserData(requestContext)).thenReturn(userData);
        when(requestContext.hasFilter()).thenReturn(false);
        when(requestContext.getFragmentIndex()).thenReturn(0);

        accessor = new HiveAccessor();
        accessor.initialize(requestContext);
        accessor.openForRead();
        accessor.readNextObject();

        verify(reader, times(1)).next(any(), any());
    }
}

class HiveUserDataBuilder {
    private String serdeClassName;
    private String partitionKeys;
    private int skipHeader;

    public HiveUserData build() {
        return new HiveUserData(
                null,
                serdeClassName,
                null,
                partitionKeys,
                false,
                null,
                null,
                skipHeader);
    }

    public HiveUserDataBuilder withSerdeClassName(String s) {
        serdeClassName = s;
        return this;
    }

    public HiveUserDataBuilder withPartitionKeys(String s) {
        partitionKeys = s;
        return this;
    }

    public HiveUserDataBuilder withSkipHeader(int n) {
        skipHeader = n;
        return this;
    }
}
