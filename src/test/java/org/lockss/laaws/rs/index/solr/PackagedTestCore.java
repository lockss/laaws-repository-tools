/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.index.solr;

import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrResponseErrorException;
import org.lockss.laaws.rs.model.ArtifactSpec;
import org.lockss.laaws.rs.model.V1Artifact;
import org.lockss.laaws.rs.model.V2Artifact;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum PackagedTestCore {
  SOLR6_EMPTY_V1(Version.fromBits(6,6,5),  false, 1),
  SOLR6_POPULATED_V1(Version.fromBits(6,6,5), true, 1);

  public static final String PACKAGED_SOLRHOME_FILELIST = "/solr/filelist.txt";

  private final static L4JLogger log = L4JLogger.getLogger();

  private final Version solrVersion;
  private final boolean populated;
  private final int configSetVersion;

  PackagedTestCore(Version solrVersion, boolean populated, int configSetVersion) {
    this.solrVersion = solrVersion;
    this.populated = populated;
    this.configSetVersion = configSetVersion;
  }

  public static List<PackagedTestCore> getCoresWithMajorVersion(int version) {
    return Arrays.stream(values())
        .filter(core -> core.getSolrVersion().major == version)
        .collect(Collectors.toList());
  }

  public static List<PackagedTestCore> getCoresWithVersion(Version version) {
    return Arrays.stream(values())
        .filter(core -> core.getSolrVersion().equals(version))
        .collect(Collectors.toList());
  }

  public Version getSolrVersion() {
    return solrVersion;
  }

  public String getCoreName() {
    return name().replace("_", "-").toLowerCase();
  }

  public boolean isPopulated() {
    return populated;
  }

  public int getConfigSetVersion() {
    return configSetVersion;
  }

  public <T> Class<T> getArtifactClass() {
    return getArtifactClass(getConfigSetVersion());
  }

  /**
   *
   * @param version The LOCKSS configuration set version.
   * @param <T>
   * @return
   */
  public static <T> Class<T> getArtifactClass(int version) {
    switch (version) {
      case 1:
        return (Class<T>) V1Artifact.class;
      case 2:
      case 3:
        return (Class<T>) V2Artifact.class;

      default:
        throw new IllegalArgumentException("Unknown LOCKSS configuration set version");
    }
  }

  private static final Path SRC_SOLR_HOME_PATH = Paths.get("src/test/resources/solr");

  /**
   * Populates a packaged test Solr core instance with artifacts, if it is meant to be populated with artifacts.
   *
   * @throws Exception
   */
  public void prepareSolrCore() throws Exception {

    // Don't populate this Solr core if it is not meant to be populated
    if (!isPopulated()) {
      return;
    }

    // Start an embedded Solr server pointing to the Solr home source directory
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(SRC_SOLR_HOME_PATH, getCoreName())) {

      // Instantiate a new SolrArtifactIndex
      ArtifactIndex index = new SolrArtifactIndex(solrClient);
      index.initIndex();

      // Populate Solr core
      for (ArtifactSpec spec : getPackagedArtifactSpecs()) {
        indexArtifact(solrClient, getArtifactBean(getConfigSetVersion(), spec));
      }

      // Shutdown the SolrArtifactIndex
      index.shutdownIndex();
    }
  }

  public Object getArtifactBean(int version, ArtifactSpec spec) {
    switch (version) {
      case 1:
        return V1Artifact.fromArtifactSpec(spec);

      case 2:
      case 3:
        return V2Artifact.fromArtifactSpec(spec);

      default:
        log.error("Unknown version");
        throw new IllegalArgumentException("Unknown version");
    }
  }

  public void indexArtifact(SolrClient solrClient, Object obj) throws IOException {
    try {
      SolrArtifactIndex.handleSolrResponse(solrClient.addBean(obj), "Problem adding artifact bean to Solr");
      SolrArtifactIndex.handleSolrResponse(solrClient.commit(), "Problem committing addition of artifact bean to Solr");
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  public static void prepareSolrData() {
    for (PackagedTestCore testCore : values()) {
      try {
        testCore.prepareSolrCore();
      } catch (Exception e) {
        log.warn("Failed to to prepare data for Solr core [name: {}]: {}", testCore.getCoreName(), e);
        e.printStackTrace();
      }
    }
  }

  public static List<ArtifactSpec> getPackagedArtifactSpecs() {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(
        ArtifactSpec.forCollAuUrlVer("c", "a", "u", 1)
            .setArtifactId("aid")
            .setCollectionDate(0)
            .setStorageUrl("sUrl")
            .setContent("hello")
//            .setContent("\"Sometimes I can't find the right words.\"\n\"Sometimes there aren't any.\"")
            .thenCommit()
    );

    return specs;
  }

  public static void main(String[] args) {
    PackagedTestCore.prepareSolrData();
  }
}
