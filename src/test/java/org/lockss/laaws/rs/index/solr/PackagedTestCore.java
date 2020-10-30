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
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum PackagedTestCore {
  // Solr 6.x
  SOLR6_EMPTY_V1("solr6-empty-v1", Version.fromBits(6,6,5), 1, false),
  SOLR6_POPULATED_V1("solr6-populated-v1", Version.fromBits(6,6,5), 1, true),
  SOLR6_EMPTY_V2("solr6-empty-v2", Version.fromBits(6,6,5), 2, false),
  SOLR6_POPULATED_V2("solr6-populated-v2", Version.fromBits(6,6,5), 2, true),

  // Solr 7.x
  SOLR7_EMPTY_V1("solr7-empty-v1", Version.fromBits(7,2,1), 1, false),
  SOLR7_POPULATED_V1("solr7-populated-v1", Version.fromBits(7,2,1), 1, true),
  SOLR7_EMPTY_V2("solr7-empty-v2", Version.fromBits(7,2,1), 2, false),
  SOLR7_POPULATED_V2("solr7-populated-v2", Version.fromBits(7,2,1), 2, true),
  SOLR7_EMPTY_V3("solr7-empty-v3", Version.fromBits(7,2,1), 3, false),
  SOLR7_POPULATED_V3("solr7-populated-v3", Version.fromBits(7,2,1), 3, true);

  // Cores from sullockss-laaws-dev-02 and sullockss-laaws-dev-04
//  LOCKSS_DEV2_20191115("dev2.lockss-repo.20191115", Version.fromBits(6, 6, 5), 1, true, false),
//  LOCKSS_DEV3_20191205("dev3.lockss-repo.20191205", Version.fromBits(6, 6, 5), 1, true, false),
//  LOCKSS_DEV4_20190829("dev4.lockss-repo.20190829", Version.fromBits(6, 6, 5), 2, true, false),
//  LOCKSS_DEV4_20190909("dev4.lockss-repo.20190909", Version.fromBits(7, 2, 1), 2, true, false);

  public static final String PACKAGED_SOLRHOME_FILELIST = "/solr/filelist.txt";

  private final static L4JLogger log = L4JLogger.getLogger();

  private final String coreName;
  private final Version solrVersion;
  private final int configSetVersion;
  private final boolean sampled;
  private final boolean populate;

  PackagedTestCore(String coreName, Version solrVersion, int configSetVersion, boolean populate) {
    this(coreName, solrVersion, configSetVersion, false, populate);
  }

  PackagedTestCore(String coreName, Version solrVersion, int configSetVersion, boolean sampled, boolean populate) {
    this.coreName = coreName;
    this.solrVersion = solrVersion;
    this.configSetVersion = configSetVersion;
    this.sampled = sampled;
    this.populate = populate;
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

  public static List<PackagedTestCore> getSampledCores() {
    return Arrays.stream(values())
        .filter(core -> core.isSampled())
        .collect(Collectors.toList());
  }

  public String getCoreName() {
    return coreName;
  }

  public Version getSolrVersion() {
    return solrVersion;
  }

  public int getConfigSetVersion() {
    return configSetVersion;
  }

  public boolean isSampled() {
    return sampled;
  }

  public boolean isPopulated() {
    return populate;
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
    if (isSampled()) {
      // Sampled: Nothing to prepare
      return;
    }

    // Solr core instance directory
    Path instanceDir = SRC_SOLR_HOME_PATH.resolve(String.format("lockss/cores/%s", getCoreName()));

    // Create core admin
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        getCoreName(),
        SRC_SOLR_HOME_PATH,
        instanceDir,
        instanceDir.resolve("conf"),
        instanceDir.resolve("data"),
        instanceDir.resolve("data/index"),
        null,
        null,
        getConfigSetVersion()
    );

    // Create the Solr core
    coreAdmin.create();

    // Start an embedded Solr server pointing to the Solr home source directory
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(SRC_SOLR_HOME_PATH, getCoreName())) {

      // Instantiate a new SolrArtifactIndex
      ArtifactIndex index = new SolrArtifactIndex(solrClient, getCoreName());
      index.initIndex();

      // Populate Solr core
      if (isPopulated()) {
        for (ArtifactSpec spec : getPackagedArtifactSpecs()) {
          indexArtifact(solrClient, getArtifactBean(getConfigSetVersion(), spec));
        }
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

  public static void indexArtifact(SolrClient solrClient, Object obj) throws IOException {
    try {
      SolrArtifactIndexAdmin.handleSolrResponse(solrClient.addBean(obj), "Problem adding artifact bean to Solr");
      SolrArtifactIndexAdmin.handleSolrResponse(solrClient.commit(), "Problem committing addition of artifact bean to Solr");
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  public static void prepareSolrData() {
    List<PackagedTestCore> pCores = getCoresWithVersion(Version.LATEST);

    if (pCores.size() > 0) {

      for (PackagedTestCore pCore : getCoresWithVersion(Version.LATEST)) {
        try {
          pCore.prepareSolrCore();
        } catch (Exception e) {
          log.warn("Failed to to prepare data for Solr core [name: {}]: {}", pCore.getCoreName(), e);
          e.printStackTrace();
        }
      }

    } else {
      log.warn("No cores to prepare for this version of Solr [version: {}]", Version.LATEST);
    }
  }

  public static List<ArtifactSpec> getPackagedArtifactSpecs() {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(
        ArtifactSpec.forCollAuUrlVer("c", "a", "u", 1)
            .setArtifactId("aid")
            .setCollectionDate(0)
            .setStorageUrl(URI.create("sUrl"))
            .setContent("\"Sometimes I can't find the right words.\"\n\"Sometimes there aren't any.\"")
            .thenCommit()
    );

    return specs;
  }

  public static void main(String[] args) {
    PackagedTestCore.prepareSolrData();
  }
}
