package org.greenplum.pxf.api.utilities;

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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.greenplum.pxf.api.model.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utilities class exposes helper method for PXF classes
 */
public class Utilities {

    private static final Logger LOG = LoggerFactory.getLogger(Utilities.class);
    private static final String PROPERTY_KEY_FRAGMENTER_CACHE = "pxf.service.fragmenter.cache.enabled";
    private static final char[] PROHIBITED_CHARS = new char[]{'/', '\\', '.', ' ', ',', ';'};
    private static final String[] HOSTS = new String[]{"localhost"};
    public static final String DEFAULT_CHARSET = "UTF-8";

    /**
     * Returns a decoded base64 byte[], or throws an error if the base64 string is invalid
     *
     * @param encoded   the base64 encoded string
     * @param paramName the name of the parameter
     * @return the decoded base64 string
     */
    public static byte[] parseBase64(String encoded, String paramName) {
        if (encoded == null) {
            return null;
        }
        if (!Base64.isArrayByteBase64(encoded.getBytes())) {
            String message = String.format("%s must be Base64 encoded. (Bad value: %s)", paramName, encoded);
            throw new IllegalArgumentException(message);
        }
        byte[] parsed = Base64.decodeBase64(encoded);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Decoded value: {}", new String(parsed));
        }
        return parsed;
    }

    /**
     * Validation for directory names that can be created
     * for the server directory.
     *
     * @param name the directory name
     * @return true if valid, false otherwise
     */
    public static boolean isValidDirectoryName(String name) {
        return isValidRestrictedDirectoryName(name, false);
    }

    /**
     * Validation for directory names that can be created
     * for the server configuration directory. Perform validation
     * for prohibited characters.
     *
     * @param name the directory name
     * @return true if valid, false otherwise
     */
    public static boolean isValidRestrictedDirectoryName(String name) {
        return isValidRestrictedDirectoryName(name, true);
    }

    /**
     * Validation for directory names that can be created
     * for the server configuration directory. Optionally checking for
     * prohibited characters in the directory name.
     *
     * @param name                    the directory name
     * @param checkForProhibitedChars true to check for prohibited chars, false otherwise
     * @return true if value, false otherwise
     */
    private static boolean isValidRestrictedDirectoryName(String name, boolean checkForProhibitedChars) {
        if (StringUtils.isBlank(name) ||
                (checkForProhibitedChars && StringUtils.containsAny(name, PROHIBITED_CHARS))) {
            return false;
        }
        File file = new File(name);
        try {
            file.getCanonicalPath();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Creates an object using the class name. The class name has to be a class
     * located in the webapp's CLASSPATH.
     *
     * @param confClass the class of the metaData used to initialize the
     *                  instance
     * @param className a class name to be initialized.
     * @param metaData  input data used to initialize the class
     * @return Initialized instance of given className
     * @throws Exception throws exception if classname was not found in
     *                   classpath, didn't have expected constructor or failed to be
     *                   instantiated
     */
    public static Object createAnyInstance(Class<?> confClass,
                                           String className, RequestContext metaData)
            throws Exception {

        Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            /* In case the class name uses the older and unsupported  "com.pivotal.pxf"
             * package name, recommend using the new package "org.greenplum.pxf"
             */
            if (className.startsWith("com.pivotal.pxf")) {
                throw new Exception(
                        "Class "
                                + className
                                + " does not appear in classpath. "
                                + "Plugins provided by PXF must start with \"org.greenplum.pxf\"",
                        e.getCause());
            } else {
                throw e;
            }
        }

        Constructor<?> con = cls.getConstructor(confClass);

        return instantiate(con, metaData);
    }

    /**
     * Creates an object using the class name with its default constructor. The
     * class name has to be a class located in the webapp's CLASSPATH.
     *
     * @param className a class name to be initialized
     * @return initialized instance of given className
     * @throws Exception throws exception if classname was not found in
     *                   classpath, didn't have expected constructor or failed to be
     *                   instantiated
     */
    public static Object createAnyInstance(String className) throws Exception {
        Class<?> cls = Class.forName(className);
        Constructor<?> con = cls.getConstructor();
        return instantiate(con);
    }

    private static Object instantiate(Constructor<?> con, Object... args)
            throws Exception {
        try {
            return con.newInstance(args);
        } catch (InvocationTargetException e) {
            /*
             * We are creating resolvers, accessors, fragmenters, etc. using the
             * reflection framework. If for example, a resolver, during its
             * instantiation - in the c'tor, will throw an exception, the
             * Resolver's exception will reach the Reflection layer and there it
             * will be wrapped inside an InvocationTargetException. Here we are
             * above the Reflection layer and we need to unwrap the Resolver's
             * initial exception and throw it instead of the wrapper
             * InvocationTargetException so that our initial Exception text will
             * be displayed in psql instead of the message:
             * "Internal Server Error"
             */
            throw (e.getCause() != null) ? new Exception(e.getCause()) : e;
        }
    }

    /**
     * Transforms a byte array into a string of octal codes in the form
     * \\xyz\\xyz
     * <p>
     * We double escape each char because it is required in postgres bytea for
     * some bytes. In the minimum all non-printables, backslash, null and single
     * quote. Easier to just escape everything see
     * http://www.postgresql.org/docs/9.0/static/datatype-binary.html
     * <p>
     * Octal codes must be padded to 3 characters (001, 012)
     *
     * @param bytes bytes to escape
     * @param sb    octal codes of given bytes
     */
    public static void byteArrayToOctalString(byte[] bytes, StringBuilder sb) {
        if ((bytes == null) || (sb == null)) {
            return;
        }

        sb.ensureCapacity(sb.length()
                + (bytes.length * 5 /* characters per byte */));
        for (int b : bytes) {
            sb.append(String.format("\\\\%03o", b & 0xff));
        }
    }

    /**
     * Replaces any non-alpha-numeric character with a '.'. This measure is used
     * to prevent cross-site scripting (XSS) when an input string might include
     * code or script. By removing all special characters and returning a
     * censured string to the user this threat is avoided.
     *
     * @param input string to be masked
     * @return masked string
     */
    public static String maskNonPrintables(String input) {
        if (StringUtils.isEmpty(input)) {
            return input;
        }
        return input.replaceAll("[^a-zA-Z0-9_:/-]", ".");
    }

    /**
     * Determines whether a class with a given name implements a specific interface.
     *
     * @param className name of the class
     * @param iface     class of the interface
     * @return true if the class implements the interface, false otherwise
     */
    public static boolean implementsInterface(String className, Class<?> iface) {
        boolean result = false;
        try {
            result = iface.isAssignableFrom(Class.forName(className));
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to load class: {}", e.getMessage());
        }
        return result;
    }

    /**
     * Returns whether fragmenter cache has been configured as enabled.
     * Defaults to true.
     *
     * @return true if fragmenter cache is enabled, false otherwise
     */
    public static boolean isFragmenterCacheEnabled() {
        return !StringUtils.equalsIgnoreCase(System.getProperty(PROPERTY_KEY_FRAGMENTER_CACHE, "true"), "false");
    }

    /**
     * Data sources are absolute data paths. Method ensures that dataSource
     * begins with '/' unless the path includes the protocol as a prefix
     *
     * @param dataSource The path to a file or directory of interest.
     *                   Retrieved from the client request.
     * @return an absolute data path
     */
    public static String absoluteDataPath(String dataSource) {
        return (dataSource.charAt(0) == '/') ? dataSource : "/" + dataSource;
    }

    /**
     * Determine whether the configuration is using Kerberos to
     * establish user identities or is relying on simple authentication
     *
     * @param configuration the configuration for a given server
     * @return true if the given configuration is for a secure environment
     */
    public static boolean isSecurityEnabled(Configuration configuration) {
        return SecurityUtil.getAuthenticationMethod(configuration) !=
                UserGroupInformation.AuthenticationMethod.SIMPLE;
    }

    /**
     * Returns the UTF-8 encoded byte array representation of the input string
     *
     * @param value the string to encode
     * @return the UTF-8 encoded byte array representation of the input string
     */
    public static byte[] getUtf8Bytes(String value) throws UnsupportedEncodingException {
        return value.getBytes(DEFAULT_CHARSET);
    }
}
