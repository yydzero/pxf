package org.greenplum.pxf.api.security;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BaseCredentials implements Credentials {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCredentials.class);

    @Override
    public String getServerCredentialsFilename(String server) {
        server = StringUtils.isBlank(server) ? "" : server.toLowerCase() + "-";
        return server + SERVER_JCEKS_FILENAME;
    }

    @Override
    public String getServerCredentialsProviderName(File serversConfigDir, String server) {
        // ensure the server directory corresponding to the server name exists
        File serverConfigDir = new File(serversConfigDir, server);
        if (! serverConfigDir.exists()) {
            throw new RuntimeException(String.format("server directory %s does not exist, create it first", serverConfigDir));
        } else if (! serverConfigDir.isDirectory()) {
            throw new RuntimeException(String.format("server directory %s is not a directory", serverConfigDir));
        }

        String providerName = LOCALJCEKS_FILE_PREFIX + new File(serverConfigDir, getServerCredentialsFilename(server)).getAbsolutePath();
        LOG.debug("Returning server credentials provider = {} for server {}", providerName, server);

        return providerName;
    }
}
