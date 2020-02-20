package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ProcessorFactory;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class handles the subpath /<version>/Controller/ of this
 * REST component
 * <p>
 * {@code
 * curl -H "X-GP-FORMAT: TEXT" \
 * -H "X-GP-DATA-DIR: tmp/dummy1" \
 * -H "X-GP-HAS-FILTER: 0" \
 * -H "X-GP-URL-HOST: localhost" \
 * -H "X-GP-URL-PORT: 5888" \
 * -H "X-GP-SEGMENT-ID: 0" \
 * -H "X-GP-SEGMENT-COUNT: 1" \
 * -H "X-GP-XID: 14-0000000004" \
 * -H "X-GP-ALIGNMENT: 8" \
 * -H "X-GP-ATTRS: 3" \
 * -H "X-GP-ATTR-NAME0: a" \
 * -H "X-GP-ATTR-TYPECODE0: 25" \
 * -H "X-GP-ATTR-TYPENAME0: text" \
 * -H "X-GP-ATTR-NAME1: b" \
 * -H "X-GP-ATTR-TYPECODE1: 25" \
 * -H "X-GP-ATTR-TYPENAME1: text" \
 * -H "X-GP-ATTR-NAME2: c" \
 * -H "X-GP-ATTR-TYPECODE2: 25" \
 * -H "X-GP-ATTR-TYPENAME2: text" \
 * -H "X-GP-USER: gpadmin" \
 * -H "X-GP-OPTIONS-FRAGMENTER: org.greenplum.pxf.api.examples.DemoFragmenter" \
 * -H "X-GP-OPTIONS-ACCESSOR: org.greenplum.pxf.api.examples.DemoAccessor" \
 * -H "X-GP-OPTIONS-RESOLVER: org.greenplum.pxf.api.examples.DemoTextResolver" \
 * -i "http://localhost:5888/pxf/v15/Controller"
 * }
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Controller/")
public class ControllerResource extends BaseResource {

    private ProcessorFactory processorFactory;

    /**
     * Creates an instance of the resource with the default singletons of
     * RequestParser, ProcessorFactory, and QuerySessionCacheFactory.
     */
    public ControllerResource() {
        this(HttpRequestParser.getInstance(), ProcessorFactory.getInstance());
    }

    /**
     * Creates an instance of the resource with provided instances of
     * RequestParser, ProcessorFactory and QuerySessionCacheFactory.
     *
     * @param parser           request parser
     * @param processorFactory a factory that constructs processors
     */
    ControllerResource(RequestParser<HttpHeaders> parser, ProcessorFactory processorFactory) {
        super(RequestContext.RequestType.READ_CONTROLLER, parser);
        this.processorFactory = processorFactory;
    }

    /**
     * Handles read data request. Parses the request, creates a bridge instance
     * and iterates over its records, printing it out to the outgoing stream.
     * Outputs GPDBWritable or Text formats.
     * <p>
     * Parameters come via HTTP headers.
     *
     * @param servletContext Servlet context contains attributes required by SecuredHDFS
     * @param headers        Holds HTTP headers from request
     * @return response object containing stream that will output records
     */
    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @SuppressWarnings("unchecked")
    public Response read(@Context final ServletContext servletContext,
                         @Context HttpHeaders headers) {

        RequestContext context = parseRequest(headers);

//        // THREAD-SAFE parameter has precedence
//        boolean isThreadSafe = context.isThreadSafe() && processor.isThreadSafe();
//        LOG.debug("Request for {} will be handled {} synchronization", context.getDataSource(), (isThreadSafe ? "without" : "with"));

        Processor<?> processor = processorFactory.getPlugin(context);
        // TODO: lock when not thread safe
        return Response
                .ok(processor, MediaType.APPLICATION_OCTET_STREAM)
                .build();
    }
}
