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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.SegmentInfos;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactSpec;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;
import org.lockss.util.test.LockssTestCase5;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestSolrArtifactIndexAdmin extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final Path SRC_SOLR_HOME_PATH = Paths.get("src/test/resources/solr");
  private static final Path MASTER_SOLR_HOME_PATH = Paths.get("target/test-classes/solr");

  private static final String SOLR6_EMPTY_V1 = "solr6-empty-v1";
  private static final String SOLR6_EMPTY_V2 = "solr6-empty-v2";

  private static final String SOLR6_POPULATED_V1 = "solr6-populated-v1";
  private static final String SOLR6_POPULATED_V2 = "solr6-populated-v2";

  private static SolrClient solrClient;
  private static Path solrHomePath;

  // JUNIT /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @BeforeAll
  public static void copyResourcesForTests() throws IOException {
    // Create a temporary directory to hold a copy of the test Solr environment
    File tmpDir = FileUtil.createTempDir("testSolrHome", null);
    tmpDir.deleteOnExit();
    solrHomePath = tmpDir.toPath();

    // Copy contents from the master copy into temporary directory created above
    FileUtils.copyDirectory(MASTER_SOLR_HOME_PATH.toFile(), tmpDir);

    log.trace("solrHomePath = {}", solrHomePath);
  }

  @AfterAll
  public static void cleanupTestResources() {
    // solrHomePath.toFile().delete();
  }

  @AfterEach
  public void shutdownEmbeddedSolrServer() throws IOException {
    // Shutdown the embedded Solr server if one was started
    if (solrClient != null) {
      solrClient.close();
    }
  }

  // UTILITIES /////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static void startEmbeddedSolrServer(Path solrHome, String defaultCore) {
    if (solrClient != null) {
      throw new IllegalStateException("An embedded Solr server is already running");
    }

    solrClient = new EmbeddedSolrServer(solrHome, defaultCore);
  }

  protected static void startMiniSolrCloudCluster() throws IOException {
    // Base directory for the MiniSolrCloudCluster
    Path tempDir = Files.createTempDirectory("MiniSolrCloudCluster");

    // Jetty configuration - Q: Is this really needed?
    JettyConfig.Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);

    try {

      // Start new MiniSolrCloudCluster with default solr.xml
      MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(1, tempDir, jettyConfig.build());

      // Upload our Solr configuration set for tests
//      cluster.uploadConfigSet(SOLR_CONFIG_PATH.toPath(), SOLR_CONFIG_NAME);

      // Get a Solr client handle to the Solr Cloud cluster
      solrClient = new CloudSolrClient.Builder()
          .withZkHost(cluster.getZkServer().getZkAddress())
          .build();

      ((CloudSolrClient)solrClient).connect();

    } catch (Exception e) {
      log.error("Could not start MiniSolrCloudCluster", e);
    }
  }

  // TESTS /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Disabled
  @Test
  public void prepareSolrData() throws Exception {
    startEmbeddedSolrServer(SRC_SOLR_HOME_PATH, SOLR6_POPULATED_V1);

    ArtifactIndex index = new SolrArtifactIndex(solrClient);

    index.initIndex();

    index.indexArtifact(
        ArtifactSpec.forCollAuUrl("c", "a", "u")
            .setArtifactId("ok")
            .setCollectionDate(0)
            .setStorageUrl("url")
            .generateContent()
            .thenCommit()
            .getArtifactData()
    );

    index.shutdownIndex();

    for (Artifact artifact : index.getArtifacts("c", "a", true)) {

      log.debug("artifact = {}", artifact);

    }

    shutdownEmbeddedSolrServer();

  }

  @Test
  public void testLocalCoreUpdater() throws Exception {

    // Get an instance of LocalCoreUpdater for "testcore" in the temporary Solr home directory
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin updater = SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeWithName(
        solrHomePath,
        SOLR6_POPULATED_V1
    );

    // This should not happen unless there's a problem in the test framework
    assertNotNull(updater);

    // Assert the index has LOCKSS config set version 1
    assertEquals(1, updater.getConfigSetVersion());

    // Assert the index minimum segment version is 6.6.5
    SegmentInfos segInfos = updater.getSegmentInfos();
    assertNotNull(segInfos);
    assertNotNull(segInfos.getMinSegmentLuceneVersion());

//    assertEquals(Version.LUCENE_6_6_2, segInfos.getMinSegmentLuceneVersion());

    updater.update();

    assertEquals(2, updater.getConfigSetVersion());

    log.trace("SECOND RUN");

    updater.update();
  }

  @Test
  public void testCoreExists() throws Exception {
    startEmbeddedSolrServer(solrHomePath, SOLR6_POPULATED_V1);

    assertTrue(SolrArtifactIndexAdmin.coreExists(solrClient, SOLR6_POPULATED_V1));
    assertFalse(SolrArtifactIndexAdmin.coreExists(solrClient, "foo"));
  }
}
