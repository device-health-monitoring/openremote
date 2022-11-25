/*
 * Copyright 2022, OpenRemote Inc.
 *
 * See the CONTRIBUTORS.txt file in the distribution for a
 * full listing of individual contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.openremote.manager.mqtt;

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.commons.lang3.tuple.Triple;
import org.keycloak.KeycloakSecurityContext;
import org.keycloak.adapters.KeycloakDeployment;
import org.openremote.container.security.keycloak.KeycloakIdentityProvider;
import org.openremote.manager.security.AuthorisationService;
import org.openremote.manager.security.MultiTenantJaasCallbackHandler;
import org.openremote.model.syslog.SyslogCategory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.activemq.artemis.core.remoting.CertificateUtil.getCertsFromConnection;
import static org.openremote.model.syslog.SyslogCategory.API;

/**
 * A security manager that uses the {@link org.openremote.manager.security.MultiTenantJaasCallbackHandler} with a
 * dynamic {@link org.keycloak.adapters.KeycloakDeployment} resolver.
 *
 * Unfortunately lots of private methods and fields in super class.
 */
public class ActiveMQORSecurityManager extends ActiveMQJAASSecurityManager {

    private static final Logger LOG = SyslogCategory.getLogger(API, ActiveMQORSecurityManager.class);
    protected AuthorisationService authorisationService;
    protected MQTTBrokerService brokerService;
    protected Function<String, KeycloakDeployment> deploymentResolver;

    // Duplicate fields due to being private in super class
    protected String certificateConfigName;
    protected String configName;
    protected SecurityConfiguration config;
    protected SecurityConfiguration certificateConfig;
    protected ActiveMQServer server;

    public ActiveMQORSecurityManager(AuthorisationService authorisationService, MQTTBrokerService brokerService, Function<String, KeycloakDeployment> deploymentResolver, String configurationName, SecurityConfiguration configuration) {
        super(configurationName, configuration);
        this.authorisationService = authorisationService;
        this.brokerService = brokerService;
        this.deploymentResolver = deploymentResolver;
        this.configName = configurationName;
        this.config = configuration;
    }

    @Override
    public Subject authenticate(String user, String password, RemotingConnection remotingConnection, String securityDomain) {
        try {
            return getAuthenticatedSubject(user, password, remotingConnection, securityDomain);
        } catch (LoginException e) {
            return null;
        }
    }

    protected Subject getAuthenticatedSubject(String user,
                                            String password,
                                            final RemotingConnection remotingConnection,
                                            final String securityDomain) throws LoginException {
        LoginContext lc;
        String realm = null;
        ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader thisLoader = this.getClass().getClassLoader();

        // A bit of a hack to allow auto provisioned clients to remain connected after authentication and to then have
        // the usual service user authentication without having to disconnect and reconnect
        if (user == null) {
            Triple<String, String, String> transientCredentials = brokerService.transientCredentials.get(remotingConnection.getID());
            if (transientCredentials != null) {
                LOG.finer("Using transient credentials connection: " + remotingConnection.getID() + ", user=" + transientCredentials.getMiddle());
                user = transientCredentials.getMiddle();
                password = transientCredentials.getRight();
            }
        }

        if (user != null) {
            String[] realmAndUsername = user.split(":");
            if (realmAndUsername.length == 2) {
                realm = realmAndUsername[0];
                user = realmAndUsername[1];
            }
        }

        try {
            if (thisLoader != currentLoader) {
                Thread.currentThread().setContextClassLoader(thisLoader);
            }
            if (securityDomain != null) {
                lc = new LoginContext(securityDomain, null, new MultiTenantJaasCallbackHandler(deploymentResolver, realm, user, password, remotingConnection), null);
            } else if (certificateConfigName != null && certificateConfigName.length() > 0 && getCertsFromConnection(remotingConnection) != null) {
                lc = new LoginContext(certificateConfigName, null, new MultiTenantJaasCallbackHandler(deploymentResolver, realm, user, password, remotingConnection), certificateConfig);
            } else {
                lc = new LoginContext(configName, null, new MultiTenantJaasCallbackHandler(deploymentResolver, realm, user, password, remotingConnection), config);
            }
            try {
                lc.login();
            } catch (LoginException e) {
                throw e;
            }
            Subject subject = lc.getSubject();

            if (subject != null) {
                // Insert remoting connection for use in authorisation
                subject.getPrincipals().add(new MQTTConnectionPrincipal(remotingConnection));
                // Ensure subject is available when afterCreateConnection is fired
                remotingConnection.setSubject(subject);
            }

            return subject;
        } finally {
            if (thisLoader != currentLoader) {
                Thread.currentThread().setContextClassLoader(currentLoader);
            }
        }
    }

    @Override
    public boolean authorize(Subject subject, Set<Role> roles, CheckType checkType, String address) {

        return switch (checkType) {
            case SEND ->true;
//                    verifyRights(subject, address, true);
            case CONSUME ->true;
//            {
//                int index = address.indexOf("::");
//                address = address.substring(0, index);
//                yield verifyRights(subject, address, false);
//            }
            case CREATE_ADDRESS, DELETE_ADDRESS, CREATE_DURABLE_QUEUE, DELETE_DURABLE_QUEUE, CREATE_NON_DURABLE_QUEUE, DELETE_NON_DURABLE_QUEUE ->
                // All MQTT clients must be able to create addresses and queues (every session and subscription will create a queue within the topic address)
                true;
            case MANAGE, BROWSE -> false;
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected boolean verifyRights(Subject subject, String address, boolean isWrite) {
        KeycloakSecurityContext securityContext = KeycloakIdentityProvider.getSecurityContext(subject);
        RemotingConnection connection = MQTTHandler.getConnectionFromSubject(subject);
        Topic topic;

        try {
            // Get MQTT topic from address
            topic = Topic.fromAddress(address, brokerService.getWildcardConfiguration());
        } catch (IllegalArgumentException e) {
            LOG.log(Level.WARNING, "Invalid topic provided by client '" + address + "', connection=" + connection, e);
            return false;
        }

        if (isWrite && topic.hasWildcard()) {
            return false;
        }

        // See if a custom handler wants to handle authorisation for this topic pub/sub
        for (MQTTHandler handler : brokerService.getCustomHandlers()) {
            if (handler.handlesTopic(topic)) {
                LOG.fine("Passing topic to handler for " + (isWrite ? "pub" : "sub") + ": handler=" + handler.getName() + ", topic=" + topic + ", connection=" + connection);
                boolean result;

                if (isWrite) {
                    result = handler.checkCanPublish(connection, securityContext, topic);
                } else {
                    result = handler.checkCanSubscribe(connection, securityContext, topic);
                }
                return result;
            }
        }

        LOG.fine("No handler has allowed " + (isWrite ? "pub" : "sub") + ": topic=" + topic + ", connection=" + connection);
        return false;
    }

}
