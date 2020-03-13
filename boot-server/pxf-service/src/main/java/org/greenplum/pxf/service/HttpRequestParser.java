package org.greenplum.pxf.service;

import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.ColumnDescriptor;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Parser for HTTP requests that contain data in HTTP headers.
 */
@Component
public class HttpRequestParser implements RequestParser<Map<String, String>> {

    private static final String TRUE_LCASE = "true";
    private static final String FALSE_LCASE = "false";

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestParser.class);
    private static final HttpRequestParser instance = new HttpRequestParser();
    private static final String PROFILE_SCHEME = "PROFILE-SCHEME";

    /**
     * Throws an exception when the given property value is missing in request.
     *
     * @param property missing property name
     * @throws IllegalArgumentException throws an exception with the property
     *                                  name in the error message
     */
    private static void protocolViolation(String property) {
        String error = String.format("Property %s has no value in the current request", property);
        throw new IllegalArgumentException(error);
    }

    @Override
    public RequestContext parseRequest(Map<String, String> request, RequestContext.RequestType requestType) {

        RequestMap params = new RequestMap(request);

        if (LOG.isDebugEnabled()) {
            // Logging only keys to prevent sensitive data to be logged
            LOG.debug("Parsing request parameters: " + params.keySet());
        }

        // build new instance of RequestContext and fill it with parsed values
        RequestContext context = new RequestContext();

        // whether we are in a fragmenter, read_bridge, or write_bridge scenario
        context.setRequestType(requestType);

        // first of all, set profile and enrich parameters with information from specified profile
        String profile = params.removeUserProperty("PROFILE");
        context.setProfile(profile);
        addProfilePlugins(profile, params);

        // Ext table uses system property FORMAT for wire serialization format
        String wireFormat = params.removeProperty("FORMAT");
        context.setOutputFormat(OutputFormat.valueOf(wireFormat));

        // FDW uses user property FORMAT to indicate format of data
        String format = params.removeUserProperty("FORMAT");
        format = StringUtils.isNotBlank(format) ? format : context.inferFormatName();
        context.setFormat(format);

        context.setAccessor(params.removeUserProperty("ACCESSOR"));

        /*
         * Some resources don't require a fragment, hence the list can be empty.
         */
        String fragmentStr = params.removeOptionalProperty("DATA-FRAGMENT");
        if (StringUtils.isNotBlank(fragmentStr)) {
            context.setDataFragment(Integer.parseInt(fragmentStr));
        }

        context.setDataSource(params.removeProperty("DATA-DIR"));

        String filterString = params.removeOptionalProperty("FILTER");
        String hasFilter = params.removeProperty("HAS-FILTER");
        if (filterString != null) {
            context.setFilterString(filterString);
        } else if ("1".equals(hasFilter)) {
            LOG.info("Original query has filter, but it was not propagated to PXF");
        }

        context.setFragmenter(params.removeUserProperty("FRAGMENTER"));

        String fragmentIndexStr = params.removeOptionalProperty("FRAGMENT-INDEX");
        if (StringUtils.isNotBlank(fragmentIndexStr)) {
            context.setFragmentIndex(Integer.parseInt(fragmentIndexStr));
        }

        String encodedFragmentMetadata = params.removeOptionalProperty("FRAGMENT-METADATA");
        context.setFragmentMetadata(Utilities.parseBase64(encodedFragmentMetadata, "Fragment metadata information"));
        context.setHost(params.removeProperty("URL-HOST"));
        context.setMetadata(params.removeUserProperty("METADATA"));
        context.setPort(params.removeIntProperty("URL-PORT"));
        context.setProfileScheme(params.removeUserProperty(PROFILE_SCHEME));
        context.setProtocol(params.removeUserProperty("PROTOCOL"));
        context.setRemoteLogin(params.removeOptionalProperty("REMOTE-USER"));
        context.setRemoteSecret(params.removeOptionalProperty("REMOTE-PASS"));
        context.setResolver(params.removeUserProperty("RESOLVER"));
        context.setSegmentId(params.removeIntProperty("SEGMENT-ID"));
        context.setServerName(params.removeUserProperty("SERVER"));

        // An optional CONFIG value specifies the name of the server
        // configuration directory, if not provided the config is the server name
        String config = params.removeUserProperty("CONFIG");
        context.setConfig(StringUtils.isNotBlank(config) ? config : context.getServerName());

        String maxFrags = params.removeUserProperty("STATS-MAX-FRAGMENTS");
        if (!StringUtils.isBlank(maxFrags)) {
            context.setStatsMaxFragments(Integer.parseInt(maxFrags));
        }

        String sampleRatioStr = params.removeUserProperty("STATS-SAMPLE-RATIO");
        if (!StringUtils.isBlank(sampleRatioStr)) {
            context.setStatsSampleRatio(Float.parseFloat(sampleRatioStr));
        }

        String threadSafeStr = params.removeUserProperty("THREAD-SAFE");
        if (!StringUtils.isBlank(threadSafeStr)) {
            context.setThreadSafe(parseBooleanValue(threadSafeStr));
        }

        context.setTotalSegments(params.removeIntProperty("SEGMENT-COUNT"));
        context.setTransactionId(params.removeProperty("XID"));

        // parse tuple description
        parseTupleDescription(params, context);

        // parse CSV format information
        parseGreenplumCSV(params, context);

        context.setUser(params.removeProperty("USER"));

        String encodedFragmentUserData = params.removeOptionalProperty("FRAGMENT-USER-DATA");
        context.setUserData(Utilities.parseBase64(encodedFragmentUserData, "Fragment user data"));

        // Store alignment for global use as a system property
        System.setProperty("greenplum.alignment", params.removeProperty("ALIGNMENT"));

        Map<String, String> optionMappings = null;

        // prepare addition configuration properties if profile was specified
        if (StringUtils.isNotBlank(profile)) {
//            optionMappings = pluginConf.getOptionMappings(profile);
        }

        // use empty map for convenience instead of null
        if (optionMappings == null) {
            optionMappings = Collections.emptyMap();
        }

        Map<String, String> additionalConfigProps = new HashMap<>();

        // Iterate over the remaining properties
        // we clone the keyset to prevent concurrent modification exceptions
        List<String> paramNames = new ArrayList<>(params.keySet());
        for (String param : paramNames) {
            if (StringUtils.startsWithIgnoreCase(param, RequestMap.USER_PROP_PREFIX)) {
                // Add all left-over user properties as options
                String optionName = param.toLowerCase().replace(RequestMap.USER_PROP_PREFIX_LOWERCASE, "");
                String optionValue = params.removeUserProperty(optionName);
                context.addOption(optionName, optionValue);
                LOG.debug("Added option {} to request context", optionName);

                // lookup if the option should also be applied as a config property
                String propertyName = optionMappings.get(optionName);
                if (StringUtils.isNotBlank(propertyName)) {
                    // if option has been provided by the user in the request, set the value
                    // of the corresponding configuration property
                    if (optionValue != null) {
                        additionalConfigProps.put(propertyName, optionValue);
                        // do not log property value as it might contain sensitive information
                        LOG.debug("Added extra config property {} from option {}", propertyName, optionName);
                    }
                }
            } else if (StringUtils.startsWithIgnoreCase(param, RequestMap.PROP_PREFIX)) {
                // log debug for all left-over system properties
                LOG.debug("Unused property {}", param);
            }
        }

        context.setAdditionalConfigProps(additionalConfigProps);
//        context.setPluginConf(pluginConf);
//
//        // Call the protocol handler for any protocol-specific logic handling
//        if (StringUtils.isNotBlank(profile)) {
//            String handlerClassName = pluginConf.getHandler(profile);
//            if (StringUtils.isNotBlank(handlerClassName)) {
//                Class<?> clazz;
//                try {
//                    clazz = Class.forName(handlerClassName);
//                    ProtocolHandler handler = (ProtocolHandler) clazz.getDeclaredConstructor().newInstance();
//                    context.setFragmenter(handler.getFragmenterClassName(context));
//                    context.setAccessor(handler.getAccessorClassName(context));
//                    context.setResolver(handler.getResolverClassName(context));
//                } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
//                        InstantiationException | IllegalAccessException e) {
//                    throw new RuntimeException(String.format("Error when invoking handlerClass '%s' : %s", handlerClassName, e), e);
//                }
//            }
//        }

        // validate that the result has all required fields, and values are in valid ranges
        context.validate();

        return context;
    }

    private void parseGreenplumCSV(RequestMap params, RequestContext context) {
        context.getGreenplumCSV()
                .withDelimiter(params.removeUserProperty("DELIMITER"))
                .withEscapeChar(params.removeUserProperty("ESCAPE"))
                .withNewline(params.removeUserProperty("NEWLINE"))
                .withQuoteChar(params.removeUserProperty("QUOTE"))
                .withValueOfNull(params.removeUserProperty("NULL"));
    }

    /**
     * Sets the requested profile plugins from the profile file into the request map.
     *
     * @param profile profile specified in the user request
     * @param params  parameters provided in the user request
     */
    private void addProfilePlugins(String profile, RequestMap params) {
        // if profile was not specified in the request, do nothing
        if (StringUtils.isBlank(profile)) {
            LOG.debug("Profile was not specified in the request");
            return;
        }

        LOG.debug("Adding plugins for profile {}", profile);

//        // get Profile's plugins from the configuration file
//        Map<String, String> pluginsMap = pluginConf.getPlugins(profile);
//
//        // create sets of keys to find out duplicates between what user has specified in the request
//        // and what is configured in the configuration file -- DO NOT ALLOW DUPLICATES
//        List<String> duplicates = pluginsMap.keySet().stream()
//                .filter(n -> params.containsKey(RequestMap.USER_PROP_PREFIX + n))
//                .collect(Collectors.toList());
//
//        if (!duplicates.isEmpty()) {
//            throw new IllegalArgumentException(String.format("Profile '%s' already defines: %s",
//                    profile, duplicates));
//        }
//
//        // since there are guaranteed to be no duplications at this point,
//        // add properties defined by profiles to the request map as if they were specified by the user
//        pluginsMap.forEach((k, v) -> params.put(RequestMap.USER_PROP_PREFIX + k, v));
//
//        params.put(RequestMap.USER_PROP_PREFIX + PROFILE_SCHEME, pluginConf.getProtocol(profile));
    }

    private boolean parseBooleanValue(String threadSafeStr) {

        if (threadSafeStr.equalsIgnoreCase(TRUE_LCASE)) {
            return true;
        }
        if (threadSafeStr.equalsIgnoreCase(FALSE_LCASE)) {
            return false;
        }
        throw new IllegalArgumentException("Illegal boolean value '"
                + threadSafeStr + "'." + " Usage: [TRUE|FALSE]");
    }

    /*
     * Sets the tuple description for the record
     * Attribute Projection information is optional
     */
    private void parseTupleDescription(RequestMap params, RequestContext context) {
        int columns = params.removeIntProperty("ATTRS");
        BitSet attrsProjected = new BitSet(columns + 1);

        /* Process column projection info */
        String columnProjStr = params.removeOptionalProperty("ATTRS-PROJ");
        if (columnProjStr != null) {
            int numberOfProjectedColumns = Integer.parseInt(columnProjStr);
            context.setNumAttrsProjected(numberOfProjectedColumns);
            if (numberOfProjectedColumns > 0) {
                String[] projectionIndices = params.removeProperty("ATTRS-PROJ-IDX").split(",");
                for (String s : projectionIndices) {
                    attrsProjected.set(Integer.valueOf(s));
                }
            } else {
                /* This is a special case to handle aggregate queries not related to any specific column
                 * eg: count(*) queries. */
                attrsProjected.set(0);
            }
        }


        for (int attrNumber = 0; attrNumber < columns; attrNumber++) {
            String columnName = params.removeProperty("ATTR-NAME" + attrNumber);
            int columnOID = params.removeIntProperty("ATTR-TYPECODE" + attrNumber);
            String columnTypeName = params.removeProperty("ATTR-TYPENAME" + attrNumber);
            Integer[] columnTypeMods = parseTypeMods(params, attrNumber);
            // Project the column if columnProjStr is null
            boolean isProjected = columnProjStr == null || attrsProjected.get(attrNumber);
            ColumnDescriptor column = new ColumnDescriptor(
                    columnName,
                    columnOID,
                    attrNumber,
                    columnTypeName,
                    columnTypeMods,
                    isProjected);
            context.getTupleDescription().add(column);

            if (columnName.equalsIgnoreCase(ColumnDescriptor.RECORD_KEY_NAME)) {
                context.setRecordkeyColumn(column);
            }
        }
    }

    private Integer[] parseTypeMods(RequestMap params, int columnIndex) {
        String typeModCountPropName = "ATTR-TYPEMOD" + columnIndex + "-COUNT";
        String typeModeCountStr = params.removeOptionalProperty(typeModCountPropName);
        if (typeModeCountStr == null)
            return null;

        int typeModeCount = parsePositiveIntOrError(typeModeCountStr, typeModCountPropName);

        Integer[] result = new Integer[typeModeCount];
        for (int i = 0; i < typeModeCount; i++) {
            String typeModItemPropName = "ATTR-TYPEMOD" + columnIndex + "-" + i;
            result[i] = parsePositiveIntOrError(params.removeProperty(typeModItemPropName), typeModItemPropName);
        }
        return result;
    }

    private int parsePositiveIntOrError(String s, String propName) {
        int n;
        try {
            n = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("%s must be an integer", propName), e);
        }
        if (n < 0) {
            throw new IllegalArgumentException(String.format("%s must be a positive integer", propName));
        }
        return n;
    }

    /**
     * Converts the request headers multivalued map to a case-insensitive
     * regular map by taking only first values and storing them in a
     * CASE_INSENSITIVE_ORDER TreeMap. All values are converted from ISO_8859_1
     * (ISO-LATIN-1) to UTF_8.
     */
    static class RequestMap extends TreeMap<String, String> {
        private static final String PROP_PREFIX = "X-GP-";
        private static final String USER_PROP_PREFIX = "X-GP-OPTIONS-";
        private static final String USER_PROP_PREFIX_LOWERCASE = "x-gp-options-";

        RequestMap(Map<String, String> requestHeaders) {
            super(String.CASE_INSENSITIVE_ORDER);
            for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
                String value = entry.getValue();

                // Converting to value UTF-8 encoding
                String key = entry.getKey();
                value = new String(value.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                LOG.trace("Key: {} Value: {}", key, value);
                put(key, value.replace("\\\"", "\""));
            }
        }

        /**
         * Returns a user defined property.
         *
         * @param userProp the lookup user property
         * @return property value as a String
         */
        private String removeUserProperty(String userProp) {
            return remove(USER_PROP_PREFIX + userProp);
        }

        /**
         * Returns the optional property value. Unlike {@link #removeProperty}, it will
         * not fail if the property is not found. It will just return null instead.
         *
         * @param property the lookup optional property
         * @return property value as a String
         */
        private String removeOptionalProperty(String property) {
            return remove(PROP_PREFIX + property);
        }

        /**
         * Returns a property value as an int type and removes it from the map
         *
         * @param property the lookup property
         * @return property value as an int type
         * @throws NumberFormatException if the value is missing or can't be
         *                               represented by an Integer
         */
        private int removeIntProperty(String property) {
            return Integer.parseInt(removeProperty(property));
        }

        /**
         * Returns the value to which the specified property is mapped and
         * removes it from the map
         *
         * @param property the lookup property key
         * @throws IllegalArgumentException if property key is missing
         */
        private String removeProperty(String property) {
            String result = remove(PROP_PREFIX + property);

            if (result == null) {
                protocolViolation(property);
            }

            return result;
        }

        /**
         * Returns a property value as boolean type. A boolean property is defined
         * as an int where 0 means false, and anything else true (like C).
         *
         * @param property the lookup property
         * @return property value as boolean
         * @throws NumberFormatException if the value is missing or can't be
         *                               represented by an Integer
         */
        private boolean removeBoolProperty(String property) {
            return removeIntProperty(property) != 0;
        }
    }

}
