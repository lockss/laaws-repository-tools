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
import org.apache.http.HttpException;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.util.ArtifactUtil;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.laaws.rs.util.NamedInputStreamResource;
import org.lockss.laaws.rs.model.Artifact;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URL;
import java.util.Iterator;
import java.util.List;


public class RestLockssRepositoryClient implements LockssRepositoryClient {
    private final static Log log = LogFactory.getLog(RestLockssRepositoryClient.class);

    private final String SEPERATOR = "/";
    private final String COLLECTION_BASE = SEPERATOR + "repos";
    private final String ARTIFACT_BASE = SEPERATOR + "artifacts";

    private RestTemplate restTemplate;
    private URL repositoryUrl;

    public RestLockssRepositoryClient(URL repositoryUrl) {
        this(repositoryUrl, new RestTemplate());
    }

    public RestLockssRepositoryClient(URL repositoryUrl, RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        this.repositoryUrl = repositoryUrl;

        // Set the buffer to false for streaming - still needed?
        //SimpleClientHttpRequestFactory factory = (SimpleClientHttpRequestFactory) this.restTemplate.getRequestFactory();
        //factory.setBufferRequestBody(false);
    }

    private String buildEndpoint(String collection) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(repositoryUrl);
        endpoint.append(COLLECTION_BASE).append(SEPERATOR).append(collection).append(ARTIFACT_BASE);

        return endpoint.toString();
    }

    private String buildEndpoint(String collection, String artifactId) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append(buildEndpoint(collection));
        endpoint.append(SEPERATOR).append(artifactId);

        return endpoint.toString();
    }

    public Iterator<String> getCollections() {
        ResponseEntity<List> response = restTemplate.exchange(
                repositoryUrl.toString() + "/repos",
                HttpMethod.GET,
                null,
                List.class
        );

        return response.getBody().iterator();
    }

    /**
     * Generates a multipart/form-data POST request for an artifact and submits it via REST to a LOCKSS repository
     *
     * TODO: Support artifact aspects
     *
     * @param artifact
     */
    public String addArtifact(Artifact artifact)
            throws IOException {

        // Get artifact identifier
        ArtifactIdentifier identifier = artifact.getIdentifier();

        // Create a multivalue map to contain the multipart parts
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("auid", identifier.getAuid());
        parts.add("uri", identifier.getUri());
        parts.add("version", 1); // TODO: Set to artifact version

        // Prepare artifact multipart headers
        HttpHeaders artifactPartHeaders = new HttpHeaders();

        // This must be set or else AbstractResource#contentLength will read the entire InputStream to determine the
        // content length, which will exhaust the InputStream.
        artifactPartHeaders.setContentLength(0); // TODO: Should be set to the length of the multipart body.
        artifactPartHeaders.setContentType(MediaType.valueOf("application/http;msgtype=response"));

        // Prepare artifact multipart body
        try {
            Resource artifactPartResource = new NamedInputStreamResource(
                    "artifact",
                    ArtifactUtil.getHttpResponseStreamFromArtifact(artifact)
            );

            // Add artifact multipart to multiparts list
            parts.add("artifact", new HttpEntity<>(artifactPartResource, artifactPartHeaders));
        } catch (HttpException e) {
            throw new IOException(e);
        }

        /*
        // Create an attach optional artifact aspects
        parts.add("aspects", new NamedByteArrayResource("aspect1", "metadata bytes1".getBytes()));
        parts.add("aspects", new NamedByteArrayResource("aspect2", "metadata bytes2".getBytes()));
        parts.add("aspects", new NamedByteArrayResource("aspect3", "metadata bytes3".getBytes()));
        */

        // POST body entity
        HttpEntity<MultiValueMap<String, Object>> multipartEntity = new HttpEntity<>(parts, null);

        // POST the multipart entity to the  Repository Service and return result
        return restTemplate.exchange(
                buildEndpoint(identifier.getCollection()),
                HttpMethod.POST,
                multipartEntity,
                String.class
        ).getBody();
    }

    public Artifact getArtifact(String collection, String artifactId) throws IOException {
        ResponseEntity<Resource> response = restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.GET,
                null,
                Resource.class);

        // Is this InputStream backed by memory? Or over a threshold, is it backed by disk?
        return ArtifactFactory.fromHttpResponseStream(response.getBody().getInputStream());
    }

    public ArtifactIndexData commitArtifact(String collection, String artifactId) {
        // Create a multivalue map to contain the multipart parts
        MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();
        parts.add("committed", true);

        return updateArtifact(collection, artifactId, parts);
    }

    private ArtifactIndexData updateArtifact(String collection, String artifactId, MultiValueMap parts) {
        // Create PUT request entity
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(parts, null);

        // Submit PUT request and return artifact index data
        return restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.PUT,
                requestEntity,
                ArtifactIndexData.class
        ).getBody();
    }

    public void deleteArtifact(String collection, String artifactId) {
        ResponseEntity<Integer> response = restTemplate.exchange(
                buildEndpoint(collection, artifactId),
                HttpMethod.DELETE,
                null,
                Integer.class
        );
    }

    public Iterator<ArtifactIndexData> queryArtifacts(UriComponentsBuilder builder) {
        ResponseEntity<List<ArtifactIndexData>> response = restTemplate.exchange(
                builder.build().encode().toUri(),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<ArtifactIndexData>>() {
                }
        );

        List<ArtifactIndexData> responseBody = response.getBody();

        return responseBody.iterator();
    }

    public Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("auid", auid);

        return queryArtifacts(builder);
    }

    public Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String uri) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("uri", uri);

        return queryArtifacts(builder);
    }

    public Iterator<ArtifactIndexData> getArtifactsWithUriPrefix(String collection, String auid, String prefix) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(buildEndpoint(collection))
                .queryParam("uri", prefix)
                .queryParam("auid", auid);

        return queryArtifacts(builder);
    }

    // List<ArtifactIndexData> getLatestArtifactsWithUri(String collection, String uri); // return latest version per AU
    // List<ArtifactIndexData> getLatestArtifactsWithUriPrefixFromAu(String collection, String auid, String uri);

    // Higher level repository operations: Implement as util methods?
    //public Page<URI> getUrisByCollectionAndAuid(String collection, String auid); // Get URLs, given AUID
    //public Page<String> getAuidByUri(String collection, String uri);
    //public Page<String> getAuidByRepository(String collection, String repo);
}