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

import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.util.Iterator;

public interface LockssRepositoryClient {
    // Artifact operations
    String addArtifact(Artifact artifact) throws IOException;
    Artifact getArtifact(String collection, String artifactId) throws IOException;
    ArtifactIndexData commitArtifact(String collection, String artifactId) throws IOException;
    void deleteArtifact(String collection, String artifactId) throws IOException;

    // Query operations
    Iterator<String> getCollections();
    Iterator<ArtifactIndexData> queryArtifacts(UriComponentsBuilder builder);
    Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid);
    Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String uri);
    Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String auid, String prefix);

    //public Page<URI> getUrisByCollectionAndAuid(String collection, String auid); // Get URLs, given AUID
    //public Page<String> getAuidByUri(String collection, String uri);
    //public Page<String> getAuidByRepository(String collection, String repo);
}
