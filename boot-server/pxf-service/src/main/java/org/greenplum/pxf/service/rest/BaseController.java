package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Base abstract implementation of the resource class, provides logger and request parser
 * to the subclasses.
 */
public abstract class BaseController {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final RequestParser<Map<String, String>> parser;

    public BaseController(RequestParser<Map<String, String>> parser) {
        this.parser = parser;
    }

    /**
     * Parses incoming request into request context
     *
     * @param headers the HTTP headers of incoming request
     * @return parsed request context
     */
    protected RequestContext parseRequest(Map<String, String> headers) {
        return parser.parseRequest(headers, RequestContext.RequestType.READ_CONTROLLER);
    }
}
