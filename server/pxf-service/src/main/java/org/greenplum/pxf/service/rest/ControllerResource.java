package org.greenplum.pxf.service.rest;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ProcessorFactory;
import org.greenplum.pxf.service.HttpRequestParser;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.processor.QuerySessionCacheFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

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
    private QuerySessionCacheFactory querySessionCacheFactory;

    /**
     * Creates an instance of the resource with the default singletons of
     * RequestParser, ProcessorFactory, and QuerySessionCacheFactory.
     */
    public ControllerResource() {
        this(HttpRequestParser.getInstance(),
                ProcessorFactory.getInstance(),
                QuerySessionCacheFactory.getInstance());
    }

    /**
     * Creates an instance of the resource with provided instances of
     * RequestParser, ProcessorFactory and QuerySessionCacheFactory.
     *
     * @param parser                   request parser
     * @param processorFactory         a factory that constructs processors
     * @param querySessionCacheFactory a factory that returns output queues for the processor
     */
    ControllerResource(RequestParser<HttpHeaders> parser,
                       ProcessorFactory processorFactory,
                       QuerySessionCacheFactory querySessionCacheFactory) {
        super(RequestContext.RequestType.READ_CONTROLLER, parser);
        this.processorFactory = processorFactory;
        this.querySessionCacheFactory = querySessionCacheFactory;
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
    public Response read(@Context final ServletContext servletContext,
                         @Context HttpHeaders headers) throws Throwable {

        RequestContext context = parseRequest(headers);
        final String cacheKey = getCacheKey(context);

//        // THREAD-SAFE parameter has precedence
//        boolean isThreadSafe = context.isThreadSafe() && processor.isThreadSafe();
//        LOG.debug("Request for {} will be handled {} synchronization", context.getDataSource(), (isThreadSafe ? "without" : "with"));

        QuerySession<?> querySession = getQuerySession(cacheKey, context);
        Processor processor = processorFactory.getPlugin(context);
        processor.setQuerySession(querySession);

        // TODO: lock when not thread safe
        return Response
                .ok(processor, MediaType.APPLICATION_OCTET_STREAM)
                .build();
    }

    /**
     * Query session holds state for the duration of the query, for all
     * segments for the same transaction, server name, data source and filter
     * string combination.
     *
     * @param cacheKey the key to the cache
     * @param context  the request context
     * @return the QuerySession object for the given key
     */
    private QuerySession<?> getQuerySession(final String cacheKey, final RequestContext context)
            throws Throwable {
        try {
            return querySessionCacheFactory.getCache().get(cacheKey, new Callable<QuerySession<?>>() {
                @Override
                public QuerySession<?> call() {
                    LOG.debug("Caching QuerySession for transactionId={} from segmentId={} with key={}",
                            context.getTransactionId(), context.getSegmentId(), cacheKey);
                    return new QuerySession<>(cacheKey, context.getTotalSegments());
                }
            });
        } catch (UncheckedExecutionException | ExecutionException e) {
            throw (e.getCause() != null) ? e.getCause() : e; // Unwrap the error
        }
    }

    /**
     * Returns a key for the QuerySession object. TransactionID is not
     * sufficient to key the cache. For the case where we have multiple
     * slices (i.e select a, b from c where a = 'part1' union all
     * select a, b from c where a = 'part2'), the query context will be
     * different for each slice, but the transactionID will be the same.
     * For that reason we must include the server name, data source and the
     * filter string as part of the QuerySession cache.
     *
     * @param context the request context
     * @return the key for the queue cache
     */
    private String getCacheKey(RequestContext context) {
        return String.format("%s:%s:%s:%s",
                context.getServerName(),
                context.getTransactionId(),
                context.getDataSource(),
                context.getFilterString());
    }
}
