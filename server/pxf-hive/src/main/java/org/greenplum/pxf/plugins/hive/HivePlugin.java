package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

class HivePlugin extends BasePlugin {

    /**
     * @return ORC file reader
     */
    Reader getOrcReader() {
        return HiveUtilities.getOrcReader(configuration, context);
    }
}
