package org.greenplum.pxf.api.model;

import org.apache.commons.codec.binary.Hex;

/**
 * A query split holds information about a data split.
 * {@link QuerySplitter} iterates over splits for a query slice.
 */
public class QuerySplit {

    /**
     * File path+name, table name, etc.
     */
    private final String resource;

    /**
     * Split metadata information (starting point + length, region location, etc.).
     */
    private byte[] metadata;

    /**
     * ThirdParty data added to a fragment. Ignored if null.
     */
    private byte[] userData;

    /**
     * Constructs a QuerySplit.
     *
     * @param resource the resource uri (file path+name, table name, etc.)
     */
    public QuerySplit(String resource) {
        this(resource, null, null);
    }

    /**
     * Constructs a QuerySplit.
     *
     * @param resource the resource uri (file path+name, table name, etc.)
     * @param metadata the meta data (starting point + length, region location, etc.).
     */
    public QuerySplit(String resource, byte[] metadata) {
        this(resource, metadata, null);
    }

    /**
     * Constructs a Fragment.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param metadata   the meta data (Starting point + length, region location, etc.).
     * @param userData   third party data added to a fragment.
     */
    public QuerySplit(String sourceName, byte[] metadata, byte[] userData) {
        this.resource = sourceName;
        this.metadata = metadata;
        this.userData = userData;
    }

    /**
     * Returns the resource (file path+name, table name, etc.)
     *
     * @return the resource
     */
    public String getResource() {
        return resource;
    }

    /**
     * Returns the query split metadata
     *
     * @return the query split metadata
     */
    public byte[] getMetadata() {
        return metadata;
    }

    /**
     * Sets metadata for the query split
     *
     * @param metadata the metadata for the query split
     */
    public void setMetadata(byte[] metadata) {
        this.metadata = metadata;
    }

    /**
     * Returns data added by the third party
     *
     * @return data added by third parties
     */
    public byte[] getUserData() {
        return userData;
    }

    /**
     * Sets the value for the user data
     *
     * @param userData third party data
     */
    public void setUserData(byte[] userData) {
        this.userData = userData;
    }

    /**
     * Returns a unique resource name for the given split
     *
     * @return a unique resource name for the given split
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(resource);
        if (metadata != null) {
            sb.append(":").append(Hex.encodeHex(metadata));
        }
        if (userData != null) {
            sb.append(":").append(Hex.encodeHex(userData));
        }
        return sb.toString();
    }
}
