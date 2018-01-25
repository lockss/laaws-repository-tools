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

import com.google.common.collect.Iterators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.springframework.http.HttpHeaders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestLocalLockssRepositoryClient {
    private final static Log log = LogFactory.getLog(TestLocalLockssRepositoryClient.class);
    private static final byte[] TEST1_BYTES = "hi".getBytes();
    private LockssRepositoryClient repo = null;

    @Before
    public void setUp() throws Exception {
        this.repo = new LocalLockssRepositoryClient();
    }

    private Artifact createRandomArtifact(String collection, String auid) {
        // Generate an artifact identifier
        ArtifactIdentifier id = new ArtifactIdentifier(
                collection,
                auid,
                "http://localhost/" + UUID.randomUUID().toString(),
                "0"
        );

        // Create an InputStream
        ByteArrayInputStream inputStream = new ByteArrayInputStream(TEST1_BYTES);

        // Custom artifact headers
        HttpHeaders metadata = new HttpHeaders();

        // HTTP status of the artifact
        BasicStatusLine statusLine = new BasicStatusLine(
                new ProtocolVersion("HTTP", 1,1),
                200,
                "OK"
        );

        // Assemble and return artifact
        return new Artifact(id, metadata, inputStream, statusLine);
    }

    @Test
    public void addArtifact() throws IOException {
        // Add an artifact
        String artifactId = repo.addArtifact(createRandomArtifact("test", "testAuid1"));
        assertNotNull(artifactId);
    }

    @Test
    public void getArtifact() throws IOException {
        String artifactId = repo.addArtifact(createRandomArtifact("test", "testAuid1"));
        assertNotNull(artifactId);
        Artifact artifact = repo.getArtifact("test", artifactId);
        assertNotNull(artifact);
    }

    @Test
    public void commitArtifact() throws IOException {
        String artifactId = repo.addArtifact(createRandomArtifact("test", "testAuid1"));
        assertNotNull(artifactId);
        ArtifactIndexData data = repo.commitArtifact("test", artifactId);
        assertTrue(data.getCommitted());
    }

    @Test
    public void deleteArtifact() throws IOException {
        String artifactId = repo.addArtifact(createRandomArtifact("test", "testAuid1"));
        assertNotNull(artifactId);
        repo.deleteArtifact("test", artifactId);
        assertNull(repo.getArtifact("test", artifactId));
    }

    @Test
    public void getCollections() throws IOException {
        for (Integer i = 0; i < 10; i++) {
            for (Integer j = 0; j < 10; j++) {
                String collectionId = String.format("TestCollection-%d", i);
                String auid = String.format("TestAuid-%d", j);
                String artifactId = repo.addArtifact(createRandomArtifact(collectionId, auid));
                assertNotNull(artifactId);
                assertNotNull(repo.commitArtifact(collectionId, artifactId));
            }
        }

        // Make sure there are ten collections and each begins with TestCollection
        assertEquals(10, Iterators.size(repo.getCollections()));
        repo.getCollections().forEachRemaining(x -> assertTrue(x.startsWith("TestCollection")));
    }

    @Test
    public void queryArtifacts() {
    }

    @Test
    public void getArtifactsInAU() {
    }

    @Test
    public void getArtifactsWithUriPrefix() {
    }

    @Test
    public void getArtifactsWithUriPrefix1() {
    }
}