package org.greenplum.pxf.api.model;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.security.BaseCredentials;
import org.greenplum.pxf.api.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;

public class BaseConfigurationFactory implements ConfigurationFactory {

    private static final BaseConfigurationFactory instance = new BaseConfigurationFactory();
    private static final String PXF_SERVER_CREDENTIAL_FILE = "";
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final File serversConfigDirectory;
    private final Credentials credentials;

    public BaseConfigurationFactory() {
        this(SERVERS_CONFIG_DIR, new BaseCredentials());
    }

    BaseConfigurationFactory(File serversConfigDirectory, Credentials credentials) {
        this.serversConfigDirectory = serversConfigDirectory;
        this.credentials = credentials;
    }

    /**
     * Returns the static instance for this factory
     *
     * @return the static instance for this factory
     */
    public static BaseConfigurationFactory getInstance() {
        return instance;
    }

    @Override
    public Configuration initConfiguration(String server, Map<String, String> additionalProperties) {
        // start with built-in Hadoop configuration that loads core-site.xml
        Configuration configuration = new Configuration();

        File[] serverDirectories = serversConfigDirectory
                .listFiles(f -> f.isDirectory() &&
                        f.canRead() &&
                        StringUtils.equalsIgnoreCase(server, f.getName()));

        String serverDirectoryName = serversConfigDirectory + server;

        if (serverDirectories == null || serverDirectories.length == 0) {
            LOG.warn("Directory {} does not exist, no configuration resources are added for server {}", serverDirectoryName, server);
        } else if (serverDirectories.length > 1) {
            throw new IllegalStateException(String.format(
                    "Multiple directories found for server %s. Server directories are expected to be case-insensitive.", server));
        } else {
            // add all site files as URL resources to the configuration, no resources will be added from the classpath
            LOG.debug("Using directory {} for server {} configuration", serverDirectoryName, server);
            addSiteFilesAsResources(configuration, server, serverDirectories[0]);
            addServerFileCredentialProvider(configuration, server, serverDirectories[0]);
        }

        // add additional properties, if provided
        if (additionalProperties != null) {
            additionalProperties.forEach(configuration::set);
        }

        return configuration;
    }

    private void addSiteFilesAsResources(Configuration configuration, String server, File directory) {
        // add all *-site.xml files inside the server config directory as configuration resources
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory.toPath(), "*-site.xml")) {
            for (Path path : stream) {
                URL resourceURL = path.toUri().toURL();
                LOG.debug("adding configuration resource from {}", resourceURL);
                configuration.addResource(resourceURL);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to read configuration for server %s from %s",
                    server, directory.getAbsolutePath()), e);
        }
    }

    private void addServerFileCredentialProvider(Configuration configuration, String server, File directory) {
        // check if the server credential file exists
        File credentialsFile = new File(directory, credentials.getServerCredentialsFilename(server));
        if (!credentialsFile.exists() || !credentialsFile.canRead()) {
            LOG.debug("Credentials file {} does not exist or is not readable, skipping", credentialsFile);
        } else {
            String provider = credentials.getServerCredentialsProviderName(directory, server);
            LOG.debug("Adding to configuration credentials provider {}", provider);
            String providers = configuration.get(CREDENTIAL_PROVIDER_PATH);
            providers = StringUtils.isBlank(providers) ? provider : providers + "," + provider;
            configuration.set(CREDENTIAL_PROVIDER_PATH, providers);
        }
    }

}
