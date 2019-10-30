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
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXmlConfig;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.model.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;
import org.lockss.util.test.LockssTestCase5;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.lockss.laaws.rs.index.solr.SolrArtifactIndexAdmin.LATEST_LOCKSS_CONFIGSET_VERSION;

public class TestSolrArtifactIndexAdmin extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  // Define our own Lucene Versions because we can't guarantee what will be available
  public static final Version LUCENE_6_6_5 = Version.fromBits(6, 6, 5);
  public static final Version LUCENE_7_2_1 = Version.fromBits(7, 2, 1);
  private static final Version LATEST_LUCENE_VERSION = LUCENE_7_2_1;

  private SolrClient solrClient;
  private static Path solrHomePath;

  // JUNIT /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private static final String PACKAGED_SOLRHOME_FILELIST = "/solr/filelist.txt";

  @BeforeEach
  public void copyResourcesForTests() throws IOException {
    // Create a temporary directory to hold a copy of the test Solr environment
    File tmpDir = FileUtil.createTempDir("testSolrHome", null);
    tmpDir.deleteOnExit();
    solrHomePath = tmpDir.toPath();

    log.trace("solrHomePath = {}", solrHomePath);

    // Read file list
    try (InputStream input = getClass().getResourceAsStream(PACKAGED_SOLRHOME_FILELIST)) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

        // Name of resource to load
        String resourceName;

        // Iterate over resource names from the list and copy each into the target directory
        while ((resourceName = reader.readLine()) != null) {
          // Source resource URL
          URL srcUrl = getClass().getResource(String.format("/solr/%s", resourceName));

          // Destination file
          File dstFile = solrHomePath.resolve(resourceName).toFile();

          // Copy resource to file
          FileUtils.copyURLToFile(srcUrl, dstFile);
        }

      }
    }
  }

  @AfterEach
  public void shutdownEmbeddedSolrServer() throws IOException {
    // Shutdown the embedded Solr server if one was started
    if (solrClient != null) {
      solrClient.close();
    }

    // Clean-up temporary directory
    solrHomePath.toFile().delete();
  }

  // UTILITIES /////////////////////////////////////////////////////////////////////////////////////////////////////////

  public SolrClient startEmbeddedSolrServer(Path solrHome, String defaultCore) {
    if (solrClient != null) {
      throw new IllegalStateException("An embedded Solr server is already running");
    }

    solrClient = new EmbeddedSolrServer(solrHome, defaultCore);

    return solrClient;
  }

  protected SolrClient startMiniSolrCloudCluster() throws IOException {
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

      ((CloudSolrClient) solrClient).connect();

    } catch (Exception e) {
      log.error("Could not start MiniSolrCloudCluster", e);
    }

    return solrClient;
  }

  // SolrArtifactIndexAdmin Tests //////////////////////////////////////////////////////////////////////////////////////////

  public static final String TEST_SOLR_COLLECTION_NAME = "testCollection";


  // LocalSolrCoreAdmin Tests //////////////////////////////////////////////////////////////////////////////////////////

  public static final String TEST_SOLR_CORE_NAME = "testCore";

  @Test
  public void testLocalSolrCoreAdmin_addSuffix() throws Exception {
    // Create a temporary directory
    File tmpDir = FileUtil.createTempDir("testAddSuffix", null);
    Path tmpDirPath = tmpDir.toPath();
    tmpDir.deleteOnExit();

    // Base file name, suffix, and path
    String baseFileName = "foo";
    String baseFileSuffix = "xyzzy";
    Path basePath = tmpDirPath.resolve(baseFileName);

    // Suffixed file name and path
    String suffixedFileName = String.format("%s.%s", baseFileName, baseFileSuffix);
    Path suffixedBasePath = tmpDirPath.resolve(suffixedFileName);

    // Sanity check
    assertFalse(basePath.toFile().exists());
    assertFalse(suffixedBasePath.toFile().exists());

    // Assert FileNotFoundException is thrown if the base directory is missing
    assertThrows(
        FileNotFoundException.class,
        () -> SolrArtifactIndexAdmin.LocalSolrCoreAdmin.addSuffix(basePath, baseFileSuffix)
    );

    // Create base directory
    FileUtils.forceMkdir(basePath.toFile());
    assertTrue(basePath.toFile().exists());

    // Sanity check: Assert suffixed directory does not exist yet
    assertFalse(suffixedBasePath.toFile().exists());

    // Add suffix to base directory
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin.addSuffix(basePath, baseFileSuffix);

    // Assert base directory moved to suffixed path
    assertFalse(basePath.toFile().exists());
    assertTrue(suffixedBasePath.toFile().exists());

    for (int i = 1; i <= 5; i++) {
      // Re-create base base directory
      FileUtils.forceMkdir(basePath.toFile());
      assertTrue(basePath.toFile().exists());

      // Add suffix to base directory again
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin.addSuffix(basePath, baseFileSuffix);

      // Assert base directory moved to expected suffixed destination
      assertFalse(basePath.toFile().exists());
      assertTrue(tmpDirPath.resolve(suffixedFileName + "." + i).toFile().exists());
    }

    // Sanity check
    for (int step = 6; step <= 11; step++) {
      assertFalse(tmpDirPath.resolve(suffixedFileName + "." + step).toFile().exists());
    }

    // Jump ahead and create a XXX.10 directory (and re-create the base directory)
    FileUtils.forceMkdir(basePath.toFile());
    FileUtils.forceMkdir(tmpDirPath.resolve(suffixedBasePath + ".10").toFile());

    // Sanity check
    assertTrue(basePath.toFile().exists());
    assertTrue(tmpDirPath.resolve(suffixedBasePath + ".10").toFile().exists());

    // Add suffix to base directory
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin.addSuffix(basePath, baseFileSuffix);

    // Assert base moved to the expected suffixed destination (i.e., XXX.11)
    assertFalse(basePath.toFile().exists());
    assertTrue(tmpDirPath.resolve(suffixedBasePath + ".11").toFile().exists());

    // Clean-up
    tmpDir.delete();
  }

  @Test
  public void testLocalSolrCoreAdmin_coreUsesCommonConfigSet() throws Exception {
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin(TEST_SOLR_CORE_NAME);

    // Assert a null common configuration set name results in coreUsesCommonConfigSet() returning false
    assertFalse(admin.coreUsesCommonConfigSet());

    // Set common configuration set name
    // FIXME: Do we really want to allow internal fields to be modified this way, even for testing?
    admin.sharedConfigSetName = "test";

    // Assert coreUsesCommonConfigSet() now returns true
    assertTrue(admin.coreUsesCommonConfigSet());
  }

  @Test
  public void testLocalSolrCoreAdmin_create() throws Exception {
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {
      // Solr core name
      String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      // Path to Solr core configuration set
      Path instanceDir = solrHomePath.resolve(String.format("lockss/cores/%s", solrCoreName));
      Path configDir = instanceDir.resolve("conf");

      // Create a LocalSolrCoreAdmin instance with bare minimum parameters
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
          solrCoreName,
          solrHomePath,
          instanceDir,
          configDir,
          null,
          null,
          null,
          null,
          version
      );

      // Assert the core does not exist yet
      assertCoreDoesNotExist(solrCoreName);

      // Create Solr core
      admin.create();

      // Assert the core exists and has the expected version
      assertCoreExistsAndConfigSetVersion(solrCoreName, version);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_createCoreWithLatest() throws Exception {
    // Assert the core does not exists yet
    assertCoreDoesNotExist(TEST_SOLR_CORE_NAME);

    // Create core
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, TEST_SOLR_CORE_NAME);

    // Assert the core exists and has the expected version
    assertCoreExistsAndConfigSetVersion(TEST_SOLR_CORE_NAME, LATEST_LOCKSS_CONFIGSET_VERSION);
  }

  @Test
  public void testLocalSolrCoreAdmin_createCoreWithVersion() throws Exception {
    // Assert that negative version number throws IllegalArgument exception
    assertThrows(
        IllegalArgumentException.class,
        () -> SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, TEST_SOLR_CORE_NAME, -1)
    );

    // Iterate over versions of LOCKSS configuration set to install
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {
      // Solr core name
      String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      // Assert the core does not exists yet
      assertCoreDoesNotExist(solrCoreName);

      // Create core
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, solrCoreName, version);

      // Assert the core exists and has the expected version
      assertCoreExistsAndConfigSetVersion(solrCoreName, version);
    }
  }

  private void assertCoreDoesNotExist(String solrCoreName) throws Exception {
    // Assert core does not exists according to Solr
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, solrCoreName)) {
      assertFalse(SolrArtifactIndexAdmin.coreExists(solrClient, solrCoreName));
    }
  }

  private void assertCoreExistsAndConfigSetVersion(String solrCoreName, int version) throws Exception {
    // Path to Solr core configuration set
    Path configDir = solrHomePath.resolve(String.format("lockss/cores/%s/conf", solrCoreName));

    // Assert core exists according to Solr
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, solrCoreName)) {
      assertTrue(SolrArtifactIndexAdmin.coreExists(solrClient, solrCoreName));
    }

    // Assert the LOCKSS configuration set version of the Solr core is equal to expected version
    assertEquals(version, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersion(configDir));
  }

  @Test
  public void testLocalSolrCoreAdmin_fromSolrCore() throws Exception {
    // Assert passing a null SolrCore results in an IllegalArgumentException being thrown
    assertThrows(IllegalArgumentException.class, () -> SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrCore(null));

    CoreContainer container = CoreContainer.createAndLoad(solrHomePath);

    Map<String, SolrCore> cores = container.getCores().stream().collect(
        Collectors.toMap(
            core -> core.getName(),
            core -> core
        )
    );

    for (PackagedTestCore pkgCore : PackagedTestCore.values()) {

      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin =
          SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrCore(cores.get(pkgCore.getName()));

      SolrArtifactIndexAdmin.LocalSolrCoreAdmin expected = expected_fromSolrCore(cores.get(pkgCore.getName()));

      assertEquals(expected, admin);

    }

    container.shutdown();
  }

  private static SolrArtifactIndexAdmin.LocalSolrCoreAdmin expected_fromSolrCore(SolrCore core) throws Exception {
    return new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        core.getName(),
        Paths.get(core.getCoreContainer().getSolrHome()),
        core.getResourceLoader().getInstancePath(),
        Paths.get(core.getResourceLoader().getConfigDir()),
        Paths.get(core.getDataDir()),
        Paths.get(core.getIndexDir()),
        core.getCoreDescriptor().getConfigSet(),

        // Determine config set directory path: Set to configuration set directory path under shared configuration set
        // base directory (as specified in solr.xml) if the core is configured to use a shared configuration set.
        core.getCoreDescriptor().getConfigSet() == null ?
            core.getCoreContainer().getNodeConfig().getConfigSetBaseDirectory() :
            SolrXmlConfig.fromSolrHome(Paths.get(core.getCoreContainer().getSolrHome()))
                .getConfigSetBaseDirectory()
                .resolve(core.getCoreDescriptor().getConfigSet()),

        SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersion(core)
    );
  }

  @Test
  public void testLocalSolrCoreAdmin_fromSolrHomeAndCoreName() throws Exception {
    // Assert SolrException is thrown if Solr home path does not exist
    assertThrows(
        SolrException.class,
        () -> SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHomePath.resolve("foo"), "foo")
    );

    // Assert unknown core name causes the method to return null
    assertNull(SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHomePath, "foo"));

    // Iterate over packaged Solr cores
    for (PackagedTestCore pkgCore : PackagedTestCore.values()) {

      // Get a packaged Solr core admin
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeAndCoreName(
          solrHomePath, pkgCore.getName()
      );

      // Assert we got the same core admin
      assertEquals(expected_fromSolrHomeAndCoreName(solrHomePath, pkgCore.getName()), admin);
    }
  }

  private static SolrArtifactIndexAdmin.LocalSolrCoreAdmin expected_fromSolrHomeAndCoreName(Path solrHome, String coreName) throws Exception {
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = null;

    CoreContainer container = CoreContainer.createAndLoad(solrHome);

    Map<String, SolrCore> cores = container.getCores().stream().collect(
        Collectors.toMap(
            core -> core.getName(),
            core -> core
        )
    );

    if (cores.containsKey(coreName)) {
      coreAdmin = expected_fromSolrCore(cores.get(coreName));
    }

    container.shutdown();

    return coreAdmin;
  }

  @Test
  public void testLocalSolrCoreAdmin_getLatestFileNameSuffix() throws Exception {
    // Create a temporary directory in which to perform the test
    File tmpDir = FileUtil.createTempDir("testGetLatestFileNameSuffix", null);
    tmpDir.deleteOnExit();
    Path tmpDirPath = tmpDir.toPath();

    // Stem file path
    Path stem = tmpDirPath.resolve("foo");

    // Assert we get -1 back because the stem does not exist
    assertEquals(-1, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLatestFileNameSuffix(stem));

    // Create target and assert we get back 0 because it now exists
    FileUtils.touch(stem.toFile());
    assertEquals(0, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLatestFileNameSuffix(stem));

    // Create sibling with suffix and assert we get back 1
    FileUtils.touch(tmpDirPath.resolve("foo.1").toFile());
    assertEquals(1, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLatestFileNameSuffix(stem));

    // Create sibling with suffix and assert we get back 10
    FileUtils.touch(tmpDirPath.resolve("foo.10").toFile());
    assertEquals(10, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLatestFileNameSuffix(stem));

    // Create sibling with non-numeric suffix and assert we still get back 10
    FileUtils.touch(tmpDirPath.resolve("foo.bar").toFile());
    assertEquals(10, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLatestFileNameSuffix(stem));

    // Clean-up temporary directory
    tmpDir.delete();
  }

  /**
   * Test for LocalSolrCoreAdmin#getLockssConfigSetVersion() and LocalSolrCoreAdmin#getLockssConfigSetVersion(Path).
   *
   * @throws Exception
   */
  @Test
  public void testLocalSolrCoreAdmin_getLockssConfigSetVersion() throws Exception {
    // Version of LOCKSS configuration set to install
    int version = 1;

    // Solr core name
    String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

    // Path to Solr core configuration set
    Path instanceDir = solrHomePath.resolve(String.format("lockss/cores/%s", solrCoreName));
    Path configDir = instanceDir.resolve("conf");

    // Create a LocalSolrCoreAdmin instance with bare minimum parameters
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        solrCoreName,
        solrHomePath,
        instanceDir,
        configDir,
        null,
        null,
        null,
        null,
        version
    );

    // Assert core does not exist yet
    assertCoreDoesNotExist(TEST_SOLR_CORE_NAME);

    // Create the Solr core
    admin.create();

    // Assert the LOCKSS configuration set version of the Solr core is equal to expected version
    assertEquals(version, admin.getLockssConfigSetVersion());

    // Assert the LOCKSS configuration set version of the Solr core is equal to expected version (from path)
    assertEquals(version, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersion(configDir));

    // TODO
//    SolrCore core = getSolrCore();
//    assertEquals(version, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersion(core));
  }

  @Test
  public void testLocalSolrCoreAdmin_getSegmentInfos() throws Exception {
    for (PackagedTestCore core : PackagedTestCore.values()) {
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = core.getCoreAdmin();

      SegmentInfos segInfos = coreAdmin.getSegmentInfos();
      assertNotNull(segInfos);

      if (core.isPopulated()) {
        assertTrue(segInfos.size() > 0);
        assertNotNull(segInfos.getMinSegmentLuceneVersion());
      } else {
        assertEquals(0, segInfos.size());
        assertNull(segInfos.getMinSegmentLuceneVersion());
      }
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_installLockssConfigSetVersion() throws Exception {
    // Iterate over versions of LOCKSS configuration sets
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {
      String coreName = String.format("testCore-%d", version);
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = createTestLocalSolrCoreAdmin(coreName);

      // Install LOCKSS configuration set version into core
      coreAdmin.installLockssConfigSetVersion(version);

      assertEquals(version, coreAdmin.getLockssConfigSetVersion());
      assertLockssConfigSetVersion(coreAdmin.configDirPath, version);
    }
  }

  private void assertLockssConfigSetVersion(Path configDir, int version) throws IOException {
    // Name of file list resource for this version
    String fileListResource = String.format("/solr/configsets/lockss/v%d/filelist.txt", version);

    // Read file list
    InputStream input = getClass().getResourceAsStream(fileListResource);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

      // Name of resource to load
      String resourceName;

      // Iterate resources from list and copy each into the Core's configuration set path
      while ((resourceName = reader.readLine()) != null) {

        // Source resource URL
        URL resUrl = getClass().getResource(
            String.format("/solr/configsets/lockss/v%d/conf/%s", version, resourceName)
        );

        // Destination file
        File dstFile = configDir.resolve(resourceName).toFile();

        // Assert destination file exists
        assertTrue(dstFile.exists());
        assertTrue(dstFile.isFile());

        // Compute checksum of resource and file
        long resChecksum = FileUtils.checksumCRC32(FileUtils.toFile(resUrl));
        long dstChecksum = FileUtils.checksumCRC32(dstFile);

        // Assert checksums match
        assertEquals(resChecksum, dstChecksum,
            String.format(
                "Checksum of file does not match resource [file: %s, fileChecksum: %d, resourceChecksum: %d]",
                dstFile,
                resChecksum,
                dstChecksum
            )
        );
      }
    }
  }

  public SolrArtifactIndexAdmin.LocalSolrCoreAdmin createTestLocalSolrCoreAdmin(String coreName) {
    return createTestLocalSolrCoreAdmin(coreName, -1);
  }

  public SolrArtifactIndexAdmin.LocalSolrCoreAdmin createTestLocalSolrCoreAdmin(String coreName, int version) {
    // Solr core instance directory
    Path instanceDir = solrHomePath.resolve(coreName);

    return new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        coreName,
        solrHomePath,
        instanceDir,
        instanceDir.resolve("conf"),
        instanceDir.resolve("data"),
        instanceDir.resolve("data/index"),
        null,
        null,
        version
    );
  }

  /**
   * Test for {@code SolrArtifactIndexAdmin.LocalSolrCoreAdmin#retireConfigSet()}.
   * <p>
   * This test also tests {@code retirePath(Path)} and {@code addSuffix(Path, String)}
   *
   * @throws Exception
   */
  @Test
  public void testLocalSolrCoreAdmin_retireConfigSet() throws Exception {
    // Create a temporary directory to serve as our instance directory
    File tmpDir = FileUtil.createTempDir("testRetireConfigSet", null);
    Path tmpDirPath = tmpDir.toPath();
    tmpDir.deleteOnExit();

    // Path to fake configuration set under core instance directory
    Path confDir = tmpDirPath.resolve("conf");
    log.trace("confDir = {}", confDir);

    // Generate expected base file name
    String timestamp = DateTimeFormatter.BASIC_ISO_DATE.format(LocalDateTime.now());
    String suffix = String.format("saved.%s", timestamp);
    String baseFileName = String.format("%s.%s", confDir.getFileName(), suffix);

    // Bare minimum LocalSolrCoreAdmin instantiation to test retireConfigSet()
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        null,
        null,
        tmpDirPath,
        confDir,
        null,
        null,
        null,
        null,
        -1
    );

    assertFalse(confDir.toFile().exists());
    // Assert FileNotFoundException is thrown if the configuration set directory is missing
    assertThrows(FileNotFoundException.class, () -> admin.retireConfigSet());

    // Create conf directory
    FileUtils.forceMkdir(confDir.toFile());
    assertTrue(confDir.toFile().exists());

    // Assert base retired path does not exist yet
    assertFalse(tmpDirPath.resolve(baseFileName).toFile().exists());

    // Retire config set
    admin.retireConfigSet();

    // Assert config set moved to retire path
    assertFalse(confDir.toFile().exists());
    assertTrue(tmpDirPath.resolve(baseFileName).toFile().exists());

    for (int i = 1; i <= 5; i++) {
      // Re-create conf directory
      FileUtils.forceMkdir(confDir.toFile());
      assertTrue(confDir.toFile().exists());

      // Retire configuration set
      admin.retireConfigSet();

      // Assert target moved to suffixed destination
      assertFalse(confDir.toFile().exists());
      assertTrue(tmpDirPath.resolve(baseFileName + "." + i).toFile().exists());
    }

    // Jump ahead and create a conf.saved.XXX.10 directory; re-create conf
    FileUtils.forceMkdir(confDir.toFile());
    FileUtils.forceMkdir(tmpDirPath.resolve(baseFileName + ".10").toFile());

    // Retire configuration set
    admin.retireConfigSet();

    // Assert target moved to correct suffixed destination (i.e., XXX.11)
    assertFalse(confDir.toFile().exists());
    assertTrue(tmpDirPath.resolve(baseFileName + ".11").toFile().exists());

    // Clean-up
    tmpDir.delete();
  }

  @Test
  public void testLocalSolrCoreAdmin_updateFromEmpty() throws Exception {
    // Solr core name
    String solrCoreName = TEST_SOLR_CORE_NAME;

    // Path to Solr core instance directory
    Path instanceDir = solrHomePath.resolve(String.format("lockss/cores/%s", solrCoreName));

    // Create a LocalSolrCoreAdmin instance with bare minimum parameters
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        solrCoreName,
        solrHomePath,
        instanceDir,
        instanceDir.resolve("conf"),
        instanceDir.resolve("data"),
        instanceDir.resolve("data/index"),
        null,
        null,
        1
    );

    // Assert the core does not exist yet
    assertCoreDoesNotExist(solrCoreName);

    // Create the Solr core with LOCKSS config set version 1
    admin.create();

    // Perform update
    admin.update();

    // Assert null minimum segment version because the index is empty / there are no segments yet
    assertNull(admin.getSegmentInfos().getMinSegmentLuceneVersion());

    // Add an artifact to the core
    try (SolrClient solrClient = startEmbeddedSolrServer(solrHomePath, solrCoreName)) {
      addArtifactFromSpec(
          solrClient,
          ArtifactSpec.forCollAuUrl("c", "a", "u")
              .setArtifactId("ok")
              .setCollectionDate(0)
              .setStorageUrl("url")
              .generateContent()
              .thenCommit()
      );
    }

    // Get Lucene segment information from index and assert the minimum version is on or after the expected version
    SegmentInfos segInfos = admin.getSegmentInfos();
    assertNotNull(segInfos);
    assertTrue(segInfos.getMinSegmentLuceneVersion().onOrAfter(LATEST_LUCENE_VERSION));

    // Get the LOCKSS configuration set version and assert it is latest
    assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, admin.getLockssConfigSetVersion());
  }

  private static void addArtifactFromSpec(SolrClient solrClient, ArtifactSpec spec) throws IOException {
    // Instantiate and initialize a SolrArtifactIndex using the embedded Solr server
    ArtifactIndex index = new SolrArtifactIndex(solrClient);
    index.initIndex();

    // Index the artifact data from spec
    index.indexArtifact(spec.getArtifactData());

    // Shutdown the Solr artifact index and underlying embedded Solr server
    index.shutdownIndex();
  }

  @Test
  public void testLocalSolrCoreAdmin_updateConfigSetNewCore() throws Exception {

    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {

      // Solr core name for this iteration
      String coreName = String.format("test-%d", version);

      // Create a LocalSolrCoreAdmin for testing
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = createTestLocalSolrCoreAdmin(coreName, version);

      // Assert the core does not exist yet
      assertCoreDoesNotExist(coreName);

      // Create the underlying core
      coreAdmin.create();

      // Assert the LOCKSS configuration set version installed is what we expect for this iteration
      assertEquals(version, coreAdmin.getLockssConfigSetVersion());

      // TODO: Add some artifacts

      // Update the configuration set
      coreAdmin.updateConfigSet();

      // Assert the installed LOCKSS configuration set is the latest version
      assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, coreAdmin.getLockssConfigSetVersion());
      assertLockssConfigSetVersion(coreAdmin.configDirPath, LATEST_LOCKSS_CONFIGSET_VERSION);

      // TODO: Verify artifacts
    }

    // TODO: Finish
  }

  @Test
  public void testLocalSolrCoreAdmin_updateConfigSet() throws Exception {
    // Iterate over packaged test cores
    for (PackagedTestCore core : PackagedTestCore.values()) {

      // Assert set of packaged artifacts is reflected core
      assertPackagedArtifacts(core);

      // Get LocalSolrCoreAdmin of packaged test core
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = core.getCoreAdmin();
      assertNotNull(coreAdmin);

      // Assert current configuration set version matches expected
      assertEquals(core.getConfigSetVersion(), coreAdmin.getLockssConfigSetVersion());

      // Upgrade the core's Lucene index
      coreAdmin.updateConfigSet();

      // Assert configuration set version matches latest
      assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, coreAdmin.getLockssConfigSetVersion());

      // Assert set of packaged artifacts is still reflected core
      assertPackagedArtifacts(core, core.getArtifactClass(LATEST_LOCKSS_CONFIGSET_VERSION));
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_updateLuceneIndex() throws Exception {
    // Iterate over packaged test cores
    for (PackagedTestCore core : PackagedTestCore.values()) {

      // Assert set of packaged artifacts is reflected core
      assertPackagedArtifacts(core);

      // Assert expected Lucene index version for this packaged core
      assertLuceneVersion(core, core.getSolrVersion());

      // Upgrade the core's Lucene index
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = core.getCoreAdmin();
      assertNotNull(coreAdmin);
      coreAdmin.updateLuceneIndex();

      // Assert Lucene index is at latest version
      assertLuceneVersion(core, LATEST_LUCENE_VERSION);

      // Assert set of packaged artifacts is still reflected core
      assertPackagedArtifacts(core);
    }
  }

  private void assertPackagedArtifacts(PackagedTestCore core) throws Exception {
    assertPackagedArtifacts(core, core.getArtifactClass());
  }

  private <T> void assertPackagedArtifacts(PackagedTestCore core, Class<T> artifactClass) throws Exception {
    log.trace("Artifact = {}", artifactClass);

    // FIXME: This is kind of hacky...
    Method fromArtifactSpec = artifactClass.getMethod("fromArtifactSpec", ArtifactSpec.class);

    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, core.getName())) {
      // List to hold expected artifacts
      List<T> expected = new ArrayList<>();

      if (core.isPopulated()) {
        // Populate expected artifacts list (by transforming test ArtifactSpecs to Artifacts)
        for (ArtifactSpec spec : getPackagedArtifactSpecs()) {
          T obj = (T) fromArtifactSpec.invoke(artifactClass, spec);
          expected.add(obj);
        }
      }

      // Query for all Solr documents (artifacts) in the index
      SolrQuery q = new SolrQuery("*:*");
      QueryResponse response = solrClient.query(q);

      // Get all artifacts in the index
      List<T> artifacts = response.getBeans(artifactClass);

      log.debug("expected = {}", expected);
      log.debug("actual = {}", artifacts);

      // Assert set of expected artifacts matches the set of artifacts in the index
      assertIterableEquals(expected, artifacts);
    }
  }

  @Deprecated
  private void assertTestV1Artifacts(PackagedTestCore core) throws Exception {
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, core.getName())) {
      // List to hold expected artifacts
      List<V1Artifact> expected = new ArrayList<>();

      if (core.isPopulated()) {
        // Build expected artifacts by transforming test ArtifactSpecs to V1Artifacts
        expected = getPackagedArtifactSpecs().stream()
            .map(spec -> V1Artifact.fromArtifactSpec(spec))
            .collect(Collectors.toList());
      }

      // Query for all Solr documents (artifacts) in the index
      SolrQuery q = new SolrQuery("*:*");
      QueryResponse response = solrClient.query(q);

      // Get all artifacts in the index
      List<V1Artifact> artifacts = response.getBeans(V1Artifact.class);

      log.debug("expected = {}", expected);
      log.debug("actual = {}", artifacts);

      // Assert the same set of V1Artifacts
      assertIterableEquals(expected, artifacts);
    }
  }


  public void assertLuceneVersion(PackagedTestCore testCore, Version expected) throws Exception {
    // Get segment infos from core
    SegmentInfos segInfos = testCore.getCoreAdmin().getSegmentInfos();

    if (testCore.isPopulated()) {
      // Assert the index minimum segment version is the expected version
      assertNotNull(segInfos);
      assertNotNull(segInfos.getMinSegmentLuceneVersion());
      assertEquals(expected, segInfos.getMinSegmentLuceneVersion());
    } else {
      // Assert no segments and null minimum segment version
      assertEquals(0, segInfos.size());
      assertNull(segInfos.getMinSegmentLuceneVersion());
    }
  }

  public enum PackagedTestCore {
    SOLR6_EMPTY_V1(LUCENE_6_6_5, "solr6-empty-v1", false, 1),
    SOLR6_POPULATED_V1(LUCENE_6_6_5, "solr6-populated-v1", true, 1);

    private final Version solrVersion;
    private final String name;
    private final boolean populated;

    private final int configSetVersion;

    PackagedTestCore(Version solrVersion, String name, boolean populated, int configSetVersion) {
      this.solrVersion = solrVersion;
      this.name = name;
      this.populated = populated;
      this.configSetVersion = configSetVersion;
    }

    public static List<PackagedTestCore> getCoresWithVersion(Version version) {
      return Arrays.stream(values())
          .filter(core -> core.getSolrVersion().equals(version))
          .collect(Collectors.toList());
    }

    public Version getSolrVersion() {
      return solrVersion;
    }

    public String getName() {
      return name;
    }

    public boolean isPopulated() {
      return populated;
    }

    public int getConfigSetVersion() {
      return configSetVersion;
    }

    /**
     * Returns an instance of LocalSolrCoreAdmin for this packaged test core from the test Solr home base directory.
     *
     * @return
     */
    public SolrArtifactIndexAdmin.LocalSolrCoreAdmin getCoreAdmin() {
      return SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHomePath, getName());
    }

    public <T> Class<T> getArtifactClass() {
      return getArtifactClass(getConfigSetVersion());
    }

    public static <T> Class<T> getArtifactClass(int version) {
      switch (version) {
        case 1:
          return (Class<T>) V1Artifact.class;
        case 2:
        case 3:
          return (Class<T>) V2Artifact.class;

        default:
          return null;
      }
    }
  }

  // TESTS /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public List<ArtifactSpec> getPackagedArtifactSpecs() {
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

  private static final Path SRC_SOLR_HOME_PATH = Paths.get("src/test/resources/solr");

  @Disabled
  @Test
  public void prepareSolrData() throws Exception {

    startEmbeddedSolrServer(SRC_SOLR_HOME_PATH, PackagedTestCore.SOLR6_POPULATED_V1.getName());

    ArtifactIndex index = new SolrArtifactIndex(solrClient);

    index.initIndex();

    for (ArtifactSpec spec : getPackagedArtifactSpecs()) {
      index.indexArtifact(spec.getArtifactData());
    }

    index.shutdownIndex();

    for (Artifact artifact : index.getArtifacts("c", "a", true)) {
      log.debug("artifact = {}", artifact);
    }

    shutdownEmbeddedSolrServer();
  }

  @Test
  public void testCoreExists() throws Exception {
    String coreName = PackagedTestCore.SOLR6_POPULATED_V1.getName();
    startEmbeddedSolrServer(solrHomePath, coreName);

    assertTrue(SolrArtifactIndexAdmin.coreExists(solrClient, coreName));
    assertFalse(SolrArtifactIndexAdmin.coreExists(solrClient, "foo"));
  }
}
