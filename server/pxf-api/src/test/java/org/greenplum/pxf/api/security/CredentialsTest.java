package org.greenplum.pxf.api.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CredentialsTest {

    @Test
    public void readStoredCrdential() throws Exception {

        // determine local resource file absolute path
        String credsFile = this.getClass().getClassLoader().getResource("creds.jceks").toURI().toString();
        // remove "file:" from the URI
        String path = credsFile.substring(5);

        Configuration conf = new Configuration();
        conf.set("clear-text", "changeme");
        conf.set("hadoop.security.credential.provider.path", "localjceks://file" + path);
        String password = new String(conf.getPassword("mypassword"));
        assertEquals("mysecret", password);
        assertEquals("changeme", String.valueOf(conf.getPassword("clear-text")));
    }
}
