package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

public abstract class BaseQuerySplitter extends BasePlugin implements QuerySplitter {

    public BaseQuerySplitter(RequestContext context, Configuration configuration) {
        initialize(context, configuration);
    }
}
