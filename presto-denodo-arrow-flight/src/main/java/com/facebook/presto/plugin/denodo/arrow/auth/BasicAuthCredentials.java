package com.facebook.presto.plugin.denodo.arrow.auth;

import com.facebook.airlift.log.Logger;
import io.grpc.Metadata;
import org.apache.arrow.flight.CallHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;

public class BasicAuthCredentials
        implements Consumer<CallHeaders>
{
    public static final String AUTHORIZATION_KEY = "Authorization";
    public static final String BASIC_AUTH_KEY = "Basic ";
    public static final String USER_AGENT_KEY = "user-agent";
    private final Logger logger = Logger.get(BasicAuthCredentials.class);
    private final String username;
    private final String password;
    private final String userAgent;

    public BasicAuthCredentials(String username, String password, String userAgent)
    {
        this.username = username;
        this.password = password;
        this.userAgent = userAgent;
    }

    @Override
    public void accept(CallHeaders callHeaders)
    {
        String credentials = username + ':' + password;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        callHeaders.insert(AUTHORIZATION_KEY, BASIC_AUTH_KEY + encodedCredentials);
        //callHeaders.insert(USER_AGENT_KEY, userAgent);
        logger.debug("Added Basic Auth header.");
    }
}