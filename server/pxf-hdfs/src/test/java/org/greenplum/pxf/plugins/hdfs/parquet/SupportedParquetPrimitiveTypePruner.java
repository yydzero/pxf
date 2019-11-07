package org.greenplum.pxf.plugins.hdfs.parquet;

import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.SupportedOperatorPruner;
import org.greenplum.pxf.api.filter.TreeVisitor;

import java.util.EnumSet;

public class SupportedParquetPrimitiveTypePruner extends SupportedOperatorPruner {
    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     */
    public SupportedParquetPrimitiveTypePruner(EnumSet<Operator> supportedOperators) {
        super(supportedOperators);
    }
}
