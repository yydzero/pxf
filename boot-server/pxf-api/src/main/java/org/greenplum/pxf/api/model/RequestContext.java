package org.greenplum.pxf.api.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.utilities.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Common configuration available to all PXF plugins. Represents input data
 * coming from client applications, such as GPDB.
 */
public class RequestContext {

    /**
     * The request type can be used to later determine whether we
     * are in a read, write or fragmenter call.
     */
    private RequestType requestType;
    public RequestType getRequestType() {
        return requestType;
    }

    /**
     * The request type can be set when parsing http parameters, etc.
     */
    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public enum RequestType {
        FRAGMENTER,
        READ_BRIDGE,
        WRITE_BRIDGE,
        READ_CONTROLLER,
        WRITE_CONTROLLER
    };

    // ----- NAMED PROPERTIES -----
    private String accessor;
    private String config;
    private int dataFragment = -1; /* should be deprecated */
    private String dataSource;
    private String fragmenter;
    private int fragmentIndex;
    private byte[] fragmentMetadata = null;
    private String filterString;
    // Profile-centric metadata
    private Object metadata;

    private OutputFormat outputFormat;
    private int port;
    private String host;
    private String token;
    private int statsMaxFragments = 0;
    private float statsSampleRatio = 0;

    /**
     * Number of attributes projected in query.
     * <p>
     * Example:
     * SELECT col1, col2, col3... : number of attributes projected - 3
     * SELECT col1, col2, col3... WHERE col4=a : number of attributes projected - 4
     * SELECT *... : number of attributes projected - 0
     */
    private int numAttrsProjected;

    private String profile;
    private String profileScheme;

    // The protocol defined at the foreign data wrapper (FDW) level
    private String protocol;
    // The format defined at the FDW foreign table level
    private String format;

    // Encapsulates CSV parsing information
    private GreenplumCSV greenplumCSV = new GreenplumCSV();

    /**
     * The name of the recordkey column. It can appear in any location in the
     * columns list. By specifying the recordkey column, the user declares that
     * he is interested to receive for every record retrieved also the the
     * recordkey in the database. The recordkey is present in HBase table (it is
     * called rowkey), and in sequence files. When the HDFS storage element
     * queried will not have a recordkey and the user will still specify it in
     * the "create external table" statement, then the values for this field
     * will be null. This field will always be the first field in the tuple
     * returned.
     */
    private ColumnDescriptor recordkeyColumn;

    private String remoteLogin;
    private String remoteSecret;
    private String resolver;
    private int segmentId;
    private String transactionId;
    /**
     * The name of the server to access. The name will be used to build
     * a path for the config files (i.e. $PXF_CONF/servers/$serverName/*.xml)
     */
    private String serverName = "default";
    private int totalSegments;
    /**
     * When false the bridge has to run in synchronized mode. default value is true.
     */
    private boolean threadSafe = true;

    private List<ColumnDescriptor> tupleDescription = new ArrayList<>();
    private String user;
    private byte[] userData;

    // ----- Additional Configuration Properties to be added to configuration for the request
    private Map<String, String> additionalConfigProps;
    // ----- USER-DEFINED OPTIONS other than NAMED PROPERTIES -----
    private Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Returns a String value of the given option or a default value if the option was not provided
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return string value of the option or default value if the option was not provided
     */
    public String getOption(String option, String defaultValue) {
        return options.getOrDefault(option, defaultValue);
    }

    /**
     * Returns an integer value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as an integer
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @return integer value of the option or default value if the option was not provided
     */
    public int getOption(String option, int defaultValue) {
        return getOption(option, defaultValue, false);
    }

    /**
     * Returns an integer value of the given option or a default value if the option was not provided.
     * Will throw an IllegalArgumentException if the option value can not be represented as an integer or
     * if the integer is negative but only natural integer was expected.
     *
     * @param option       name of the option
     * @param defaultValue default value
     * @param naturalOnly  true if the integer is expected to be non-negative (natural), false otherwise
     * @return integer value of the option or default value if the option was not provided
     */
    public int getOption(String option, int defaultValue, boolean naturalOnly) {
        int result = defaultValue;
        String value = options.get(option);
        if (value != null) {
            try {
                result = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a%s integer", option, value, naturalOnly ? " non-negative" : "n"), e);
            }
            if (naturalOnly && result < 0) {
                throw new IllegalArgumentException(String.format(
                        "Property %s has incorrect value %s : must be a non-negative integer", option, value));
            }
        }
        return result;
    }

    /**
     * Returns a string value of the given option or null if the option was not provided.
     *
     * @param option name of the option
     * @return string value of the given option or null if the option was not provided.
     */
    public String getOption(String option) {
        return options.get(option);
    }

    /**
     * Adds an option with the given name and value to the set of options.
     *
     * @param name  name of the option
     * @param value value of the option
     */
    public void addOption(String name, String value) {
        options.put(name, value);
    }

    /**
     * Returns unmodifiable map of options.
     *
     * @return map of options, with keys as option names and values as option values
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    public String getRemoteLogin() {
        return remoteLogin;
    }

    public void setRemoteLogin(String remoteLogin) {
        this.remoteLogin = remoteLogin;
    }

    public String getRemoteSecret() {
        return remoteSecret;
    }

    public void setRemoteSecret(String remoteSecret) {
        this.remoteSecret = remoteSecret;
    }

    public byte[] getUserData() {
        return userData;
    }

    public void setUserData(byte[] userData) {
        this.userData = userData;
    }

    /**
     * The byte serialization of a data fragment.
     *
     * @return serialized fragment metadata
     */
    public byte[] getFragmentMetadata() {
        return fragmentMetadata;
    }

    /**
     * Sets the byte serialization of a fragment meta data.
     *
     * @param fragmentMetadata start, len, and location of the fragment
     */
    public void setFragmentMetadata(byte[] fragmentMetadata) {
        this.fragmentMetadata = fragmentMetadata;
    }

    /**
     * Gets any custom user data that may have been passed from the fragmenter.
     * Will mostly be used by the accessor or resolver.
     *
     * @return fragment user data
     */
    public byte[] getFragmentUserData() {
        return userData;
    }

    /**
     * Sets any custom user data that needs to be shared across plugins. Will
     * mostly be set by the fragmenter.
     *
     * @param userData user data
     */
    public void setFragmentUserData(byte[] userData) {
        this.userData = userData;
    }

    /**
     * Returns the number of segments in GPDB.
     *
     * @return number of segments
     */
    public int getTotalSegments() {
        return totalSegments;
    }

    public void setTotalSegments(int totalSegments) {
        this.totalSegments = totalSegments;
    }

    /**
     * Returns the current segment ID in GPDB.
     *
     * @return current segment ID
     */
    public int getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    /**
     * Returns true if there is a filter string to parse.
     *
     * @return whether there is a filter string
     */
    public boolean hasFilter() {
        return filterString != null;
    }

    /**
     * Returns true if there is column projection.
     *
     * @return true if there is column projection, false otherwise
     */
    public boolean hasColumnProjection() {
        return numAttrsProjected > 0 && numAttrsProjected < tupleDescription.size();
    }

    /**
     * Returns the filter string, <tt>null</tt> if #hasFilter is <tt>false</tt>.
     *
     * @return the filter string or null
     */
    public String getFilterString() {
        return filterString;
    }

    public void setFilterString(String filterString) {
        this.filterString = filterString;
    }

    /**
     * Returns tuple description.
     *
     * @return tuple description
     */
    public List<ColumnDescriptor> getTupleDescription() {
        return tupleDescription;
    }

    public void setTupleDescription(List<ColumnDescriptor> tupleDescription) {
        this.tupleDescription = tupleDescription;
    }

    /**
     * Returns the number of columns in tuple description.
     *
     * @return number of columns
     */
    public int getColumns() {
        return tupleDescription.size();
    }

    /**
     * Returns column index from tuple description.
     *
     * @param index index of column
     * @return column by index
     */
    public ColumnDescriptor getColumn(int index) {
        return tupleDescription.get(index);
    }

    /**
     * Returns the name of the server configuration for this request.
     *
     * @return (optional) the name of the server configuration for this request
     */
    public String getConfig() {
        return config;
    }

    /**
     * Sets the name of the server configuration for this request.
     *
     * @param config the directory name for the configuration
     */
    public void setConfig(String config) {
        if (StringUtils.isNotBlank(config) && !Utilities.isValidDirectoryName(config)) {
            fail("invalid CONFIG directory name '%s'", config);
        }
        this.config = config;
    }

    /**
     * Returns the column descriptor of the recordkey column. If the recordkey
     * column was not specified by the user in the create table statement will
     * return null.
     *
     * @return column of record key or null
     */
    public ColumnDescriptor getRecordkeyColumn() {
        return recordkeyColumn;
    }

    public void setRecordkeyColumn(ColumnDescriptor recordkeyColumn) {
        this.recordkeyColumn = recordkeyColumn;
    }

    /**
     * Returns the data source of the required resource (i.e a file path or a
     * table name).
     *
     * @return data source
     */
    public String getDataSource() {
        return dataSource;
    }

    /**
     * Sets the data source for the required resource.
     *
     * @param dataSource data source to be set
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Returns the profile name.
     *
     * @return name of profile
     */
    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    /**
     * Returns the ClassName for the java class that was defined as Accessor.
     *
     * @return class name for Accessor
     */
    public String getAccessor() {
        return accessor;
    }

    public void setAccessor(String accessor) {
        this.accessor = accessor;
    }

    /**
     * Returns the ClassName for the java class that was defined as Resolver.
     *
     * @return class name for Resolver
     */
    public String getResolver() {
        return resolver;
    }

    public void setResolver(String resolver) {
        this.resolver = resolver;
    }

    /**
     * Returns the ClassName for the java class that was defined as Fragmenter
     * or null if no fragmenter was defined.
     *
     * @return class name for Fragmenter or null
     */
    public String getFragmenter() {
        return fragmenter;
    }

    public void setFragmenter(String fragmenter) {
        this.fragmenter = fragmenter;
    }

    /**
     * Returns the ClassName for the java class that was defined as Metadata
     * or null if no metadata was defined.
     *
     * @return class name for METADATA or null
     */
    public Object getMetadata() {
        return metadata;
    }

    public void setMetadata(Object metadata) {
        this.metadata = metadata;
    }

    /**
     * Returns the contents of pxf_remote_service_login set in GPDB. Should the
     * user set it to an empty string this function will return null.
     *
     * @return remote login details if set, null otherwise
     */
    public String getLogin() {
        return remoteLogin;
    }

    /**
     * Returns the contents of pxf_remote_service_secret set in GPDB. Should the
     * user set it to an empty string this function will return null.
     *
     * @return remote password if set, null otherwise
     */
    public String getSecret() {
        return remoteSecret;
    }

    /**
     * Returns whether this request is thread safe.
     * If it is not, request will be handled consequentially and not in parallel.
     *
     * @return whether the request is thread safe
     */
    public boolean isThreadSafe() {
        return threadSafe;
    }

    public void setThreadSafe(boolean threadSafe) {
        this.threadSafe = threadSafe;
    }

    /**
     * Returns a data fragment index. plan to deprecate it in favor of using
     * getFragmentMetadata().
     *
     * @return data fragment index
     */
    public int getDataFragment() {
        return dataFragment;
    }

    public void setDataFragment(int dataFragment) {
        this.dataFragment = dataFragment;
    }

    /**
     * Returns the format of the external file
     *
     * @return format of the external file
     */
    public String getFormat() {
        return format;
    }

    /**
     * Sets the format of the external file
     *
     * @param format of the external file
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * Returns index of a fragment in a file
     *
     * @return index of a fragment
     */
    public int getFragmentIndex() {
        return fragmentIndex;
    }

    /**
     * Sets index of a fragment in a file
     *
     * @param fragmentIndex index of a fragment
     */
    public void setFragmentIndex(int fragmentIndex) {
        this.fragmentIndex = fragmentIndex;
    }

    /**
     * Returns number of attributes projected in a query
     *
     * @return number of attributes projected
     */
    public int getNumAttrsProjected() {
        return numAttrsProjected;
    }

    /**
     * Sets number of attributes projected
     *
     * @param numAttrsProjected number of attributes projected
     */
    public void setNumAttrsProjected(int numAttrsProjected) {
        this.numAttrsProjected = numAttrsProjected;
    }

    /**
     * Returns the name of the server in a multi-server setup
     *
     * @return the name of the server
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Sets the name of the server in a multi-server setup.
     * If the name is blank, it is defaulted to "default"
     *
     * @param serverName the name of the server
     */
    public void setServerName(String serverName) {
        if (StringUtils.isNotBlank(serverName)) {

            if (!Utilities.isValidRestrictedDirectoryName(serverName)) {
                throw new IllegalArgumentException(String.format("Invalid server name '%s'", serverName));
            }

            this.serverName = serverName.toLowerCase();
        }
    }

    /**
     * Returns identity of the end-user making the request.
     *
     * @return userid
     */
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Returns the server port providing the service.
     *
     * @return server port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port sets the port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Returns the GreenplumCSV object encapsulating the CSV information
     *
     * @return the {@link GreenplumCSV} object
     */
    public GreenplumCSV getGreenplumCSV() {
        return greenplumCSV;
    }

    /**
     * @param greenplumCSV the {@link GreenplumCSV} object
     */
    public void setGreenplumCSV(GreenplumCSV greenplumCSV) {
        this.greenplumCSV = greenplumCSV;
    }

    /**
     * Returns the current output format, either {@link OutputFormat#TEXT} or
     * {@link OutputFormat#GPDBWritable}.
     *
     * @return output format
     */
    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    /**
     * Returns the server name providing the service.
     *
     * @return server name
     */
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns Kerberos token information.
     *
     * @return token
     */
    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    /**
     * Statistics parameter. Returns the max number of fragments to return for
     * ANALYZE sampling. The value is set in GPDB side using the GUC
     * pxf_stats_max_fragments.
     *
     * @return max number of fragments to be processed by analyze
     */
    public int getStatsMaxFragments() {
        return statsMaxFragments;
    }

    public void setStatsMaxFragments(int statsMaxFragments) {
        this.statsMaxFragments = statsMaxFragments;
        if (statsMaxFragments <= 0) {
            throw new IllegalArgumentException(String
                    .format("Wrong value '%d'. STATS-MAX-FRAGMENTS must be a positive integer",
                            statsMaxFragments));
        }
    }

    /**
     * Statistics parameter. Returns a number between 0.0001 and 1.0,
     * representing the sampling ratio on each fragment for ANALYZE sampling.
     * The value is set in GPDB side based on ANALYZE computations and the
     * number of sampled fragments.
     *
     * @return sampling ratio
     */
    public float getStatsSampleRatio() {
        return statsSampleRatio;
    }

    public void setStatsSampleRatio(float statsSampleRatio) {
        this.statsSampleRatio = statsSampleRatio;
        if (statsSampleRatio < 0.0001 || statsSampleRatio > 1.0) {
            throw new IllegalArgumentException(
                    "Wrong value '"
                            + statsSampleRatio
                            + "'. "
                            + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        }
    }

    public void validate() {
        if ((statsSampleRatio > 0) != (statsMaxFragments > 0)) {
            fail("Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        }

        // accessor and resolver are user properties, might be missing if profile is not set
        ensureNotNull("ACCESSOR", accessor);
        ensureNotNull("RESOLVER", resolver);
    }

    private void ensureNotNull(String property, Object value) {
        if (value == null) {
            fail("Property %s has no value in the current request", property);
        }
    }

    private void fail(String message, Object... args) {
        String errorMessage = String.format(message, args);
        throw new IllegalArgumentException(errorMessage);
    }

    public String getProfileScheme() {
        return profileScheme;
    }

    public void setProfileScheme(String profileScheme) {
        this.profileScheme = profileScheme;
    }

    public String getProtocol() {
        return StringUtils.isNotBlank(protocol) ? protocol : inferProtocolName();
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Map<String, String> getAdditionalConfigProps() {
        return additionalConfigProps;
    }

    public void setAdditionalConfigProps(Map<String, String> additionalConfigProps) {
        this.additionalConfigProps = additionalConfigProps;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Infers the protocol name from the profileScheme or profile
     * Introduced for backwards compatibility. Can be removed after
     * the external framework is no longer supported
     *
     * @return the inferred protocol name
     */
    private String inferProtocolName() {
        if (StringUtils.isBlank(profileScheme) && !StringUtils.isBlank(profile)) {
            return profile.contains(":") ? profile.split(":")[0] : profile;
            // When the profileScheme is not available, extract the profileScheme from the profile
            // for example hdfs:text will return hdfs profileScheme
        }
        return profileScheme;
    }

    /**
     * Infers the format name from the profile.
     * Introduced for backwards compatibility. Can be removed after
     * the external framework is no longer supported
     *
     * @return the inferred format name
     */
    public String inferFormatName() {
        if (!StringUtils.isBlank(profile) && profile.contains(":")) {
            return profile.split(":")[1];
        }
        // if the format name cannot be inferred from the profile
        // we return null
        return null;
    }
}
