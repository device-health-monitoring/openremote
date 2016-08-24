/*
 * Copyright 2016, OpenRemote Inc.
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
package org.openremote.manager.server.asset;

import org.openremote.container.web.WebResource;
import org.openremote.manager.shared.asset.Asset;
import org.openremote.manager.shared.asset.AssetResource;
import org.openremote.manager.shared.http.RequestParams;

import javax.ws.rs.BeanParam;

public class AssetResourceImpl extends WebResource implements AssetResource {

    protected final AssetService assetService;

    public AssetResourceImpl(AssetService assetService) {
        this.assetService = assetService;
    }

    @Override
    public Asset[] getRoot(@BeanParam RequestParams requestParams) {
        return assetService.getRoot();
    }

    @Override
    public Asset[] getChildren(@BeanParam RequestParams requestParams, String parentId) {
        return assetService.getChildren(parentId);
    }

    @Override
    public Asset get(@BeanParam RequestParams requestParams, String assetId) {
        return assetService.get(assetId);
    }
}
