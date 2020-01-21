package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Enhances HdfsDataFragmenter Fragment data by adding the Parquet schema
 * to the fragment
 */
public class ParquetDataFragmenter extends HdfsDataFragmenter {

    private ParquetSchemaUtility parquetSchemaUtility;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        this.parquetSchemaUtility = new ParquetSchemaUtility(context);
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        Path path = new Path(hcfsType.getDataUri(jobConf, context));
        List<InputSplit> splits = getSplits(path);

        byte[] schemaData = null;

        LOG.debug("Total number of fragments = {}", splits.size());
        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String filepath = fsp.getPath().toString();
            String[] hosts = fsp.getLocations();

//            if (schemaData == null) {
//                MessageType readSchema = parquetSchemaUtility
//                        .getReadSchema(fsp, configuration, context.getTupleDescription());
//                String readSchemaString = readSchema.toString();
//                schemaData = readSchemaString.getBytes(StandardCharsets.UTF_8);
//                LOG.trace("{}-{}: Read schema for all fragments is: {}",
//                        context.getTransactionId(), context.getSegmentId(), readSchemaString);
//            }

            /*
             * metadata information includes: file split's start, length and
             * hosts (locations).
             */
            byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata, schemaData);
            fragments.add(fragment);
        }

        return fragments;
    }
}
