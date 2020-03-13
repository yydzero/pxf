package org.greenplum.pxf.api.factory;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;

public interface ConfigurationFactory {

    String PXF_CONF_PROPERTY = "pxf.conf";
    File SERVERS_CONFIG_DIR = new File(
            System.getProperty(PXF_CONF_PROPERTY) + File.separator + "servers");
    String DEFAULT_SERVER_CONFIG_DIR = SERVERS_CONFIG_DIR + File.separator + "default";
    String PXF_CONFIG_RESOURCE_PATH_PROPERTY = "pxf.config.resource.path";
    String PXF_CONFIG_SERVER_DIRECTORY_PROPERTY = "pxf.config.server.directory";
    String PXF_SESSION_USER_PROPERTY = "pxf.session.user";

    /**
     * Initializes a configuration object that applies server-specific configurations and
     * adds additional properties on top of it, if specified.
     *
     * @param configDirectory name of the configuration directory
     * @param serverName name of the server
     * @param userName name of the user
     * @param additionalProperties additional properties to be added to the configuration
     * @return configuration object
     */
    Configuration initConfiguration(String configDirectory, String serverName, String userName, Map<String, String> additionalProperties);
}
