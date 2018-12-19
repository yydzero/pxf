package org.greenplum.pxf.api.security;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.greenplum.pxf.api.model.ConfigurationFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;
import static org.greenplum.pxf.api.security.Credentials.LOCALJCEKS_FILE_PREFIX;
import static org.greenplum.pxf.api.security.Credentials.SERVER_JCEKS_FILENAME;

/**
 * Class responsible for managing credentials in local JCEKS keystores for PXF servers
 */
public class CredentialShell {

    private Credentials credentials;

    public CredentialShell(Credentials credentials) {
        this.credentials = credentials;
    }

    public static void main(String[] args) {
        int returnCode = 0;

        CredentialShell shell = new CredentialShell(new BaseCredentials());
        try {
            shell.processRequest(args);
        } catch (UsageException e) {
            String message = e.getMessage();
            if (StringUtils.isNotBlank(message)) {
                System.err.println("ERROR: " + message);
            }
            shell.showUsage();
            returnCode = 2;
        } catch (Throwable t) {
            System.err.println("ERROR: " + t.getMessage());
            returnCode = 1;
        }
        System.exit(returnCode);
    }

    private void processRequest(String[] args) {

        // check if the PXF_CONF property was set and the mentioned server directory exists
        String configDirName = System.getProperty(ConfigurationFactory.PXF_CONF_PROPERTY);
        if (StringUtils.isBlank(configDirName)) {
            throw new RuntimeException(String.format("property %s is not set", ConfigurationFactory.PXF_CONF_PROPERTY));
        }

        // arguments expected:
        // <server-name> <command> [<alias> <value>]

        if (args.length == 0) {
            throw new UsageException();
        }

        String server = parseArg(args, 0, "server", false);

        String command = parseArg(args, 1, "command", false);
        switch (command.toLowerCase()) {
            case "create":
                String alias = parseArg(args, 2, "alias", false);
                String secret = parseArg(args, 3, "value", false);
                System.out.printf("Creating credential: %s for server: %s%n", alias, server);
                createCredential(server, alias, secret);
                System.out.printf("%s has been successfully created.%n", alias);
                break;
            case "list":
                alias = parseArg(args, 2, "alias", true);
                System.out.printf("Listing aliases for server: %s%n", server);
                listCredentials(server, alias).forEach(System.out::println);
                System.out.println();
                break;
            case "delete":
                alias = parseArg(args, 2, "alias", false);
                System.out.printf("Deleting credential: %s from server: %s%n", alias, server);
                deleteCredential(server, alias);
                System.out.printf("%s has been successfully deleted.%n", alias);
                break;
            default:
                throw new UsageException("unknown command " + command);
        }
    }

    private void createCredential(String server, String alias, String secret) {
        CredentialProvider provider = getCredentialProvider(server);
        try {
            provider.createCredentialEntry(alias, secret.toCharArray());
            provider.flush();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private List<String> listCredentials(String server, String alias) {
        CredentialProvider provider = getCredentialProvider(server);
        List<String> aliases;
        try {
            aliases = provider.getAliases();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        Collections.sort(aliases);
        return aliases;
    }

    private void deleteCredential(String server, String alias) {
        CredentialProvider provider = getCredentialProvider(server);
        try {
            provider.deleteCredentialEntry(alias);
            provider.flush();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private CredentialProvider getCredentialProvider(String server) {
        // start with the brand new configuration and add the target server-level provider to the path
        Configuration configuration = new Configuration();
        configuration.set(CREDENTIAL_PROVIDER_PATH, credentials.getServerCredentialsProviderName(ConfigurationFactory.SERVERS_CONFIG_DIR, server));

        List<CredentialProvider> providers;
        try {
            providers = CredentialProviderFactory.getProviders(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        if (providers == null || providers.size() != 1) {
            throw new RuntimeException("no credential providers are available");
        }

        return providers.get(0);
    }


    private String parseArg(String[] args, int index, String argName, boolean optional) {
        String result = null;
        if (args.length < index + 1) {
            if (!optional) {
                throw new UsageException(argName + " is missing");
            }
        } else {
            result = args[index].trim();
        }
        return result;
    }

    private void showUsage() {
        System.out.println("USAGE:");
        System.out.println("pxf credential -s <server-name> create <alias> <value>");
        System.out.println("pxf credential -s <server-name> list [<alias>]");
        System.out.println("pxf credential -s <server-name> delete <alias>");
        System.out.println("Example: pxf credential -s myserver create access_key 1A2B3C4D5E");
    }

    private static class UsageException extends RuntimeException {
        public UsageException() {

        }

        public UsageException(String message) {
            super(message);
        }
    }
}
