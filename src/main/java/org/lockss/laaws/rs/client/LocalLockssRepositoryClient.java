/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactStore;
import org.lockss.laaws.rs.io.storage.mock.VolatileWARCArtifactStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.util.Iterator;

public class LocalLockssRepositoryClient implements LockssRepositoryClient {
    private final static Log log = LogFactory.getLog(LocalLockssRepositoryClient.class);
    private ArtifactStore store = null;
    private ArtifactIndex index = null;

    public LocalLockssRepositoryClient() {
        this(new VolatileArtifactIndex(), new VolatileWARCArtifactStore());
    }

    public LocalLockssRepositoryClient(ArtifactIndex index, ArtifactStore store) {
        this.index = index;
        this.store = store;
    }

    @Override
    public String addArtifact(Artifact artifact) throws IOException {
        store.addArtifact(artifact);
        ArtifactIndexData indexData = index.indexArtifact(artifact);
        return indexData.getId();
    }

    @Override
    public Artifact getArtifact(String collection, String artifactId) throws IOException {
        return store.getArtifact(index.getArtifactIndexData(artifactId));
    }

    @Override
    public ArtifactIndexData commitArtifact(String collection, String artifactId) throws IOException {
        // Get artifact as it is currently
        ArtifactIndexData indexData = index.getArtifactIndexData(artifactId);
        Artifact artifact = store.getArtifact(indexData);

        // Record the changed status in store
        store.updateArtifact(indexData, artifact);

        // Update the commit status in index
        return index.commitArtifact(artifactId);
    }

    @Override
    public void deleteArtifact(String collection, String artifactId) throws IOException {
        store.deleteArtifact(index.getArtifactIndexData(artifactId));
        index.deleteArtifact(artifactId);
    }

    @Override
    public Iterator<String> getCollections() {
        return index.getCollectionIds();
    }

    @Override
    public Iterator<ArtifactIndexData> queryArtifacts(UriComponentsBuilder builder) {
        return null;
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid) {
        return null;
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String uri) {
        return null;
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String auid, String prefix) {
        return null;
    }
}
