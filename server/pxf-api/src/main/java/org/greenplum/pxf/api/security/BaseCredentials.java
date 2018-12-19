package org.greenplum.pxf.api.security;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BaseCredentials implements Credentials {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCredentials.class);

    @Override
    public String getServerCredentialsFilename(String server) {
        server = StringUtils.isBlank(server) ? "" : server.toLowerCase();
        return server + SERVER_JCEKS_SUFFIX;
    }

    @Override
    public String getServerCredentialsProviderName(File serverConfigDir, String server) {
        String providerName = LOCALJCEKS_FILE_PREFIX +
                new File(serverConfigDir, getServerCredentialsFilename(server)).getAbsolutePath();
        LOG.debug("Returning server credentials provider = {} for server {}", providerName, server);

        return providerName;
    }
}
