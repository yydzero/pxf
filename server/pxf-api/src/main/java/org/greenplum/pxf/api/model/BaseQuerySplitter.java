package org.greenplum.pxf.api.model;

public abstract class BaseQuerySplitter extends BasePlugin implements QuerySplitter {

    public BaseQuerySplitter(RequestContext context) {
        initialize(context);
    }
}
