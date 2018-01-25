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
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactUtil;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.*;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URL;

import static org.junit.Assert.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@RunWith(SpringRunner.class)
//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"security.basic.enabled=false"})
//@RestClientTest(RestLockssRepositoryClient.class)
@WebMvcTest
//@AutoConfigureMockMvc(secure = false)
public class TestRestLockssRepositoryClient {
    private final static Log log = LogFactory.getLog(TestRestLockssRepositoryClient.class);
    private final static byte[] TEST1_BYTES = "hello world".getBytes();

//    @LocalServerPort
//    private int port;

    private RestLockssRepositoryClient repo;
    private MockRestServiceServer server;
    private RestTemplate template;

    @Before
    public void setUp() throws Exception {
        template = new RestTemplate();
//        server = MockRestServiceServer.bindTo(template).build();
        server = MockRestServiceServer.createServer(template);
        repo = new RestLockssRepositoryClient(new URL("http://localhost"), template);

    }

    @Test
    public void getCollections() throws Exception {
//        // Generate an artifact identifier
//        ArtifactIdentifier id = new ArtifactIdentifier(
//            "test",
//            "testAuid1",
//            "http://localhost/hello1",
//            "0"
//        );
//
//        // Create an InputStream
//        ByteArrayInputStream inputStream = new ByteArrayInputStream(TEST1_BYTES);
//
//        // Custom artifact headers
//        HttpHeaders metadata = new HttpHeaders();
//
//        //
//        BasicStatusLine statusLine = new BasicStatusLine(
//                new ProtocolVersion("HTTP", 1,1),
//                200,
//                "OK"
//        );
//
//        // Add an artifact
//        String artifactId = repo.addArtifact(
//                id.getCollection(),
//                id.getAuid(),
//                id.getUri(),
//                id.getVersion(),
//                new Artifact(id, metadata, inputStream, statusLine)
//        );
//
//        // Commit artifact
//        repo.updateArtifact(id.getCollection(), artifactId);
//
//        repo.getCollections().forEachRemaining(x -> log.error(x));

        this.server.expect(requestTo("http://localhost/repos"))
                .andRespond(withSuccess("[\"hello\"]", MediaType.APPLICATION_JSON));

        repo.getCollections().forEachRemaining(x -> log.error(x));
    }


    @Test
    public void addArtifact() throws Exception {
    }

    @Test
    public void getArtifact() throws Exception {
        // Generate an artifact identifier
        ArtifactIdentifier id = new ArtifactIdentifier(
                "test",
                "testAuid1",
                "http://localhost/hello1",
                "0"
        );

        // Create an InputStream
        ByteArrayInputStream inputStream = new ByteArrayInputStream(TEST1_BYTES);

        // Custom artifact headers
        HttpHeaders metadata = new HttpHeaders();

        //
        BasicStatusLine statusLine = new BasicStatusLine(
                new ProtocolVersion("HTTP", 1, 1),
                200,
                "OK"
        );

        // Assemble the artifact
        Artifact artifact = new Artifact(id, metadata, inputStream, statusLine);

        // Encode the artifact a HTTP response stream -- we'll use it to get a byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ArtifactUtil.writeHttpResponseStream(artifact, baos);

        // Setup the expected response when fetching the artifact with artifact ID "test" from the collection "demo"
        this.server.expect(requestTo("http://localhost/repos/demo/artifacts/test"))
                .andRespond(withSuccess(baos.toByteArray(), MediaType.parseMediaType("application/http; msgtype=response")));

        // Retrieve the artifact via repository client
        Artifact returnedArtifact = repo.getArtifact("demo", "test");

        server.verify();

        assertNotNull(returnedArtifact);

        log.info(String.format(
                "Returned: [%s, %s, %s, %s]",
                returnedArtifact.getIdentifier().getCollection(),
                returnedArtifact.getIdentifier().getAuid(),
                returnedArtifact.getIdentifier().getUri(),
                returnedArtifact.getIdentifier().getVersion()
        ));

        //assertTrue(artifact.equals(returnedArtifact));
    }

    @Test
    public void commitArtifact() throws Exception {
    }

    @Test
    public void deleteArtifact() throws Exception {
    }

    @Test
    public void getArtifactsInAU() throws Exception {
    }

    @Test
    public void deleteArtifactsInAU() throws Exception {
    }

    @Test
    public void getArtifactsWithUri() throws Exception {
    }

    @Test
    public void getArtifactsWithUriPrefix() throws Exception {
    }
}