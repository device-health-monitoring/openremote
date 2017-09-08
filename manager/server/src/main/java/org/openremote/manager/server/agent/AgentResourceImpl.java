/*
 * Copyright 2017, OpenRemote Inc.
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
package org.openremote.manager.server.agent;

import org.openremote.container.timer.TimerService;
import org.openremote.manager.server.asset.AssetResourceImpl;
import org.openremote.manager.server.asset.AssetStorageService;
import org.openremote.manager.server.asset.ServerAsset;
import org.openremote.manager.server.security.ManagerIdentityService;
import org.openremote.manager.server.web.ManagerWebResource;
import org.openremote.manager.shared.agent.AgentResource;
import org.openremote.manager.shared.http.RequestParams;
import org.openremote.manager.shared.security.Tenant;
import org.openremote.model.asset.Asset;
import org.openremote.model.asset.AssetAttribute;
import org.openremote.model.asset.AssetType;
import org.openremote.model.asset.agent.ProtocolDescriptor;
import org.openremote.model.attribute.AttributeRef;
import org.openremote.model.attribute.AttributeValidationResult;
import org.openremote.model.file.FileInfo;
import org.openremote.model.util.Pair;

import javax.ws.rs.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.openremote.model.util.TextUtil.isNullOrEmpty;

public class AgentResourceImpl extends ManagerWebResource implements AgentResource {

    private static final Logger LOG = Logger.getLogger(AssetResourceImpl.class.getName());
    protected final AgentService agentService;
    protected final AssetStorageService assetStorageService;

    public AgentResourceImpl(TimerService timerService,
                             ManagerIdentityService identityService,
                             AssetStorageService assetStorageService,
                             AgentService agentService) {
        super(timerService, identityService);
        this.agentService = agentService;
        this.assetStorageService = assetStorageService;
    }

    @Override
    public ProtocolDescriptor[] getSupportedProtocols(RequestParams requestParams, String agentId) {
        Asset[] agentFinal = new Asset[1];

        return findAgent(agentId)
            .flatMap(agent -> {
                agentFinal[0] = agent;
                return agentService.getAgentConnector(agent);
            })
            .map(agentConnector -> {
                LOG.finer("Asking connector '" + agentConnector.getClass().getSimpleName() + "' for protocol descriptors");
                return agentConnector.getProtocolDescriptors(agentFinal[0]);
            })
            .orElseThrow(() -> {
                LOG.warning("Agent connector not found for agent ID: " + agentId);
                return new IllegalStateException("Agent connector not found or returned invalid response");
            });
    }

    @Override
    public Map<String, ProtocolDescriptor[]> getAllSupportedProtocols(RequestParams requestParams) {
        Map<String, ProtocolDescriptor[]> agentDescriptorMap = new HashMap<>(agentService.getAgents().size());
        agentService.getAgents().forEach((id, agent) ->
            agentDescriptorMap.put(
                id,
                agentService.getAgentConnector(agent)
                    .map(agentConnector -> {
                        LOG.finer("Asking connector '" + agentConnector.getClass().getSimpleName() + "' for protocol descriptors");
                        return agentConnector.getProtocolDescriptors(agent);
                    })
                    .orElseThrow(() -> {
                        LOG.warning("Agent connector not found for agent ID: " + id);
                        return new IllegalStateException("Agent connector not found or returned invalid response");
                    })
            ));

        return agentDescriptorMap;
    }

    @Override
    public AssetAttribute[] getDiscoveredProtocolConfigurations(RequestParams requestParams, String agentId, String protocolName) {
        return new AssetAttribute[0];
    }

    @Override
    public AttributeValidationResult validateProtocolConfiguration(RequestParams requestParams, String agentId, AssetAttribute protocolConfiguration) {
        return findAgent(agentId)
            .flatMap(agentService::getAgentConnector)
            .map(agentConnector -> {
                LOG.finer("Asking connector '" + agentConnector.getClass().getSimpleName() + "' to validate protocol configuration: " + protocolConfiguration);
                return agentConnector.validateProtocolConfiguration(protocolConfiguration);
            })
            .orElseThrow(() -> {
                LOG.warning("Agent connector not found for agent ID: " + agentId);
                return new IllegalStateException("Agent connector not found or returned invalid response");
            });
    }

    @Override
    public Asset[] getDiscoveredLinkedAttributes(RequestParams requestParams, String agentId, String protocolConfigurationName, String parentId, String realmId) {
        AttributeRef protocolConfigRef = new AttributeRef(agentId, protocolConfigurationName);
        AgentConnector agentConnector = findAgent(agentId)
            .flatMap(agentService::getAgentConnector)
            .orElseThrow(() -> new NotFoundException("Agent connector not found for: " + agentId));

        Pair<Asset, String> parentAndRealmId = getParentAssetAndRealmId(parentId, realmId);

        LOG.finer("Asking connector '" + agentConnector.getClass().getSimpleName() + "' to do linked attribute discovery for protocol configuration: " + protocolConfigRef);

        try {
            // TODO: Allow user to select which assets/attributes are actually added to the DB
            Asset[] assets = agentConnector.getDiscoveredLinkedAttributes(protocolConfigRef);
            persistAssets(assets, parentAndRealmId.key, parentAndRealmId.value);
            return assets;
        } catch (IllegalArgumentException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new NotFoundException(e.getMessage());
        } catch (UnsupportedOperationException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new NotSupportedException(e.getMessage());
        }
    }

    @Override
    public Asset[] getDiscoveredLinkedAttributes(RequestParams requestParams, String agentId, String protocolConfigurationName, String parentId, String realmId, FileInfo fileInfo) {
        AttributeRef protocolConfigRef = new AttributeRef(agentId, protocolConfigurationName);
        AgentConnector agentConnector = findAgent(agentId)
            .flatMap(agentService::getAgentConnector)
            .orElseThrow(() -> new NotFoundException("Agent connector not found for: " + agentId));

        if (fileInfo == null || fileInfo.getContents() == null) {
            throw new BadRequestException("A file must be provided for import");
        }

        Pair<Asset, String> parentAndRealmId = getParentAssetAndRealmId(parentId, realmId);

        LOG.finer("Asking connector '" + agentConnector.getClass().getSimpleName() + "' to do linked attribute discovery using uploaded file for protocol configuration: " + protocolConfigRef);

        try {
            // TODO: Allow user to select which assets/attributes are actually added to the DB
            Asset[] assets = agentConnector.getDiscoveredLinkedAttributes(protocolConfigRef, fileInfo);
            persistAssets(assets, parentAndRealmId.key, parentAndRealmId.value);
            return assets;
        } catch (IllegalArgumentException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new NotFoundException(e.getMessage());
        } catch (UnsupportedOperationException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new NotSupportedException(e.getMessage());
        } catch (IllegalStateException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new InternalServerErrorException(e.getMessage());
        }
    }

    protected Optional<Asset> findAgent(String agentId) {
        // Find the agent
        LOG.finer("Find agent: " + agentId);
        Asset agent = agentService.getAgents().get(agentId);
        if (agent == null || agent.getWellKnownType() != AssetType.AGENT) {
            LOG.warning("Failed to find agent with ID: " + agentId);
            throw new IllegalArgumentException("Agent not found");
        }
        return Optional.of(agent);
    }

    /**
     * Parent takes priority over realm ID (only super user can add to other realms)
     */
    protected Pair<Asset, String> getParentAssetAndRealmId(String parentId, String realmId) {
        if (isRestrictedUser()) {
            throw new ForbiddenException("User is restricted");
        }

        // Assets must be added in the same realm as the user (unless super user)
        Asset parentAsset = isNullOrEmpty(parentId) ? null : assetStorageService.find(parentId);

        if (parentAsset == null && !isNullOrEmpty(parentId)) {
            // Either invalid asset or user doesn't have access to it
            LOG.info("User is trying to import with an invalid or inaccessible parent");
            throw new BadRequestException("Parent either doesn't exist or is not accessible");
        }

        Tenant tenant = parentAsset != null ?
            identityService.getIdentityProvider().getTenantForRealmId(parentAsset.getRealmId()) :
            !isNullOrEmpty(realmId) ?
                identityService.getIdentityProvider().getTenantForRealmId(realmId) :
                getAuthenticatedTenant();

        if (!isTenantActiveAndAccessible(tenant)) {
            String msg = "The requested parent asset or realm is inaccessible";
            LOG.fine(msg);
            throw new ForbiddenException(msg);
        }

        return new Pair<>(parentAsset, tenant.getId());
    }

    protected void persistAssets(Asset[] assets, Asset parentAsset, String realmId) {
        if (assets == null || assets.length == 0) {
            LOG.info("No assets to import");
            return;
        }

        for (int i=0; i< assets.length; i++) {
            Asset asset = assets[i];
            asset.setId(null);
            asset.setParent(parentAsset);
            asset.setRealmId(realmId);
            ServerAsset serverAsset = ServerAsset.map(asset, new ServerAsset());
            assets[i] = assetStorageService.merge(serverAsset);
        }
    }
}
