package org.greenplum.pxf.api.security;

import java.io.File;

public interface Credentials {
    String SERVER_JCEKS_FILENAME = "server.jceks";
    String LOCALJCEKS_FILE_PREFIX = "localjceks://file";

    String getServerCredentialsFilename(String server);
    String getServerCredentialsProviderName(File serversConfigDir, String server);
}
