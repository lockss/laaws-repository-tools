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

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXmlConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.index.solr.SolrResponseErrorException;
import org.lockss.laaws.rs.model.ArtifactSpec;
import org.lockss.laaws.rs.model.V1Artifact;
import org.lockss.laaws.rs.model.V2Artifact;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
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
import static org.lockss.laaws.rs.index.solr.SolrArtifactIndexAdmin.LocalSolrCoreAdmin.REINDEX_LOCK_FILE;
import static org.lockss.laaws.rs.index.solr.SolrArtifactIndexAdmin.LocalSolrCoreAdmin.UPGRADE_LOCK_FILE;

public class TestSolrArtifactIndexAdmin extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  // Define our own latest Lucene Version because we can't guarantee what will be available
  private static final Version LATEST_LUCENE_VERSION = Version.fromBits(7, 2, 1);

  // JUNIT /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private static Path solrHomePath;

  /**
   * Before each test, install the packaged Solr home via resources to a temporary directory on disk.
   *
   * @throws IOException
   */
  @BeforeEach
  public void copyResourcesForTests() throws IOException {
    // Create a temporary directory to hold a copy of the test Solr environment
    File tmpDir = getTempDir();
    solrHomePath = tmpDir.toPath();

    log.trace("solrHomePath = {}", solrHomePath);

    // Read file list
    try (InputStream input = getClass().getResourceAsStream(PackagedTestCore.PACKAGED_SOLRHOME_FILELIST)) {
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

  // UTILITIES: EmbeddedSolrServer and MiniSolrCloudCluster startup ////////////////////////////////////////////////////

  /**
   * Starts an embedded Solr server using the provided Solr home base directory and default Solr core.
   *
   * @param solrHome    A {@link Path} to a Solr home base directory.
   * @param defaultCore A {@link String} containing the name of the default Solr core for the embedded Solr server.
   * @return A {@link EmbeddedSolrServer} instance.
   */
  @Deprecated
  private SolrClient startEmbeddedSolrServer(Path solrHome, String defaultCore) {
    return new EmbeddedSolrServer(solrHome, defaultCore);
  }

  /**
   * Starts an {@link MiniSolrCloudCluster} for testing Solr Cloud operations.
   *
   * @return A {@link MiniSolrCloudCluster} instance.
   * @throws IOException
   */
  private CloudSolrClient startMiniSolrCloudCluster() throws Exception {
    // Base directory for the MiniSolrCloudCluster
    Path tempDir = Files.createTempDirectory("MiniSolrCloudCluster");

    // Jetty configuration - Q: Is this really needed?
    JettyConfig.Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);

    // Start new MiniSolrCloudCluster with default solr.xml
    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(1, tempDir, jettyConfig.build());

    // Upload our Solr configuration set for tests
//    cluster.uploadConfigSet(SOLR_CONFIG_PATH.toPath(), SOLR_CONFIG_NAME);

    // Get a Solr client handle to the Solr Cloud cluster
    CloudSolrClient solrClient = new CloudSolrClient.Builder()
        .withZkHost(cluster.getZkServer().getZkAddress())
        .build();

    solrClient.connect();

    solrClient.close();

    return solrClient;
  }

  // UTILITIES: Asserts ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Asserts that a Solr core by the given name does not exist under the test Solr home base directory.
   *
   * @param solrCoreName A {@link String} containing the Solr core name to assert non-existence.
   * @throws Exception
   */
  private void assertCoreDoesNotExist(String solrCoreName) throws Exception {
    // Assert core does not exists according to Solr
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, solrCoreName)) {
      assertFalse(SolrArtifactIndexAdmin.coreExists(solrClient, solrCoreName));
    }
  }

  /**
   * Asserts that a Solr core exists under the test Solr home base directory, and that the expected LOCKSS configuration
   * set version is installed. The latter is checked by reading the value of the LOCKSS configuration set version key
   * from the userProps map in the configuration overlay file.
   *
   * @param solrCoreName    A {@link String} containing the name of the Solr core.
   * @param expectedVersion A {@code int} containing the expected LOCKSS configuration set version.
   * @throws Exception
   */
  private void assertLockssConfigSetVersion(String solrCoreName, int expectedVersion) throws Exception {
    // Path to Solr core configuration set
    Path configDir = solrHomePath.resolve(String.format("lockss/cores/%s/conf", solrCoreName));

    // Assert core exists according to Solr
    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, solrCoreName)) {
      assertTrue(SolrArtifactIndexAdmin.coreExists(solrClient, solrCoreName));

      // Assert the LOCKSS configuration set version of the Solr core is equal to expected version (via JSON parsing)
      assertEquals(expectedVersion, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersionFromOverlay(configDir));

      // Assert the LOCKSS configuration set version of the Solr core is equal to expected version (via SolrCore object)
      SolrCore core = solrClient.getCoreContainer().getCore(solrCoreName);
      assertEquals(expectedVersion, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersionFromSolrCore(core));
    }
  }

  /**
   * Asserts that the configuration set a given path contains the expected set of files and correct checksums for a
   * specified version of the LOCKSS configuration set.
   *
   * @param configDir A {@link Path} containing the path to a configuration set.
   * @param version   A {@code int} containing the LOCKSS configuration set version to assert.
   * @throws IOException
   */
  private void assertLockssConfigSet(Path configDir, int version) throws IOException {
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

  /**
   * Asserts the minimum segment version among the Lucene segments in a packaged Solr test core.
   *
   * @param core     A {@link PackagedTestCore} packaged Solr test core.
   * @param expected The expected minimum Lucene segment {@link Version}.
   * @throws Exception
   */
  private void assertMinSegmentLuceneVersion(PackagedTestCore core, Version expected) throws Exception {
    // Get segment infos from core
    SegmentInfos segInfos = adminFromPackagedCore(core).getSegmentInfos();

    if (core.isPopulated() || core.isSampled()) {
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

  /**
   * Asserts that a {@link PackagedTestCore} contains the expected set of artifacts used to prepare the core.
   *
   * @param core A {@link PackagedTestCore} to assert contains the expected set of artifacts used to create the core.
   * @throws Exception
   */
  private void assertPackagedArtifacts(PackagedTestCore core) throws Exception {
    if (core.isPopulated()) {
      assertPackagedArtifacts(core, core.getArtifactClass());
    }
  }

  /**
   * Asserts that a {@link PackagedTestCore} contains the expected set of artifacts used to prepare the core.
   *
   * @param core          A {@link PackagedTestCore} to assert contains the expected set of artifacts used to create the core.
   * @param artifactClass A {@code Class<T>} containing the type of the artifact in the packaged test
   * @param <T>
   * @throws Exception
   */
  private <T> void assertPackagedArtifacts(PackagedTestCore core, Class<T> artifactClass) throws Exception {
    log.trace("Artifact = {}", artifactClass);

    // FIXME: This is kind of hacky...
    Method fromArtifactSpec = artifactClass.getMethod("fromArtifactSpec", ArtifactSpec.class);

    try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHomePath, core.getCoreName())) {
      // New empty list to hold expected artifacts
      List<T> expected = new ArrayList<>();

      if (core.isPopulated()) {
        // Yes: Populate expected artifacts list from list of packaged artifact specs
        for (ArtifactSpec spec : PackagedTestCore.getPackagedArtifactSpecs()) {
          // Invoke fromArtifactSpec(...) and add versioned artifact interpretation to expected list
          T obj = (T) fromArtifactSpec.invoke(artifactClass, spec);
          expected.add(obj);
        }
      }

      // Query for all Solr documents (artifacts) in the index
      SolrQuery q = new SolrQuery("*:*");
      QueryResponse response = solrClient.query(q);

      // Get all artifacts in the index using the versioned artifact class
      List<T> artifacts = response.getBeans(artifactClass);

      log.debug("expected = {}", expected);
      log.debug("actual = {}", artifacts);

      // Assert set of expected artifacts matches the set of artifacts in the index
      assertIterableEquals(expected, artifacts);
    }
  }

  /**
   * Adds an artifact bean to a the index of the default Solr core of a SolrClient connection.
   *
   * @param solrClient An implementation of {@link SolrClient}.
   * @param obj        An artifact bean {@code Object}.
   * @throws IOException
   */
  private static void indexArtifactBean(SolrClient solrClient, Object obj) throws IOException {
    try {
      SolrArtifactIndexAdmin.handleSolrResponse(solrClient.addBean(obj), "Problem adding artifact bean to Solr");
      SolrArtifactIndexAdmin.handleSolrResponse(solrClient.commit(), "Problem committing addition of artifact bean to Solr");
    } catch (SolrResponseErrorException | SolrServerException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns an Solr artifact bean object, provided an {@link ArtifactSpec} and the target LOCKSS configuration set
   * version (i.e., schema version).
   *
   * @param version A {@code int} containing the target LOCKSS configuration set version.
   * @param spec    The {@link ArtifactSpec} that will be interpreted as a versioned artifact bean.
   * @return The artifact bean {@link Object}.
   */
  private static Object getArtifactBean(int version, ArtifactSpec spec) {
    switch (version) {
      case 1:
        return V1Artifact.fromArtifactSpec(spec);

      case 2:
      case 3:
        return V2Artifact.fromArtifactSpec(spec);

      default:
        log.error("Unknown LOCKSS configuration set version [version: {}]", version);
        throw new IllegalArgumentException("Unknown version");
    }
  }

  /**
   * Creates a {@link SolrArtifactIndexAdmin.LocalSolrCoreAdmin} representing a default Solr core structure, for testing
   * purposes only.
   * <p>
   * The Solr core is not created by this operation.
   *
   * @param coreName A {@link String} containing the name of the Solr core.
   * @return A {@link SolrArtifactIndexAdmin.LocalSolrCoreAdmin} instance representing a Solr core that does not exist.
   */
  private SolrArtifactIndexAdmin.LocalSolrCoreAdmin createTestLocalSolrCoreAdmin(String coreName, int version) {
    // Solr core instance directory
    Path instanceDir = solrHomePath.resolve(String.format("lockss/cores/%s", coreName));

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
   * Returns an instance of LocalSolrCoreAdmin for this packaged test core from the test Solr home base directory.
   *
   * @return
   */
  private SolrArtifactIndexAdmin.LocalSolrCoreAdmin adminFromPackagedCore(PackagedTestCore core) {
    return SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHomePath, core.getCoreName());
  }

  /**
   * Creates a {@link SolrArtifactIndexAdmin.LocalSolrCoreAdmin} provided the name of the core and the Solr home base
   * directory under which the core's instance directory resides.
   *
   * @param solrHome A {@link Path} to the Solr home base directory.
   * @param coreName A {@link String} containing the name of the Solr core.
   * @return A {@link SolrArtifactIndexAdmin.LocalSolrCoreAdmin} instance or {@code null} if the core doesn't exist.
   * @throws Exception
   */
  private static SolrArtifactIndexAdmin.LocalSolrCoreAdmin
  expected_fromSolrHomeAndCoreName(Path solrHome, String coreName) throws Exception {

    CoreContainer container = CoreContainer.createAndLoad(solrHome);
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = expected_fromSolrCore(container.getCore(coreName));
    container.shutdown();
    return coreAdmin;
  }

  /**
   * Creates a {@link SolrArtifactIndexAdmin.LocalSolrCoreAdmin} provided a {@link SolrCore} object.
   *
   * @param core A {@link SolrCore} instance.
   * @return A {@link SolrArtifactIndexAdmin.LocalSolrCoreAdmin} to administrate the provided {@link SolrCore}.
   * @throws Exception
   */
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

        SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersionFromSolrCore(core)
    );
  }

  // TESTS: SolrCloudArtifactIndexAdmin ////////////////////////////////////////////////////////////////////////////////

  public static final String TEST_SOLR_COLLECTION_NAME = "testCollection";

  // TODO

  // TESTS: LocalSolrCoreAdmin /////////////////////////////////////////////////////////////////////////////////////////

  public static final String TEST_SOLR_CORE_NAME = "testCore";

  @Test
  public void testLocalSolrCoreAdmin_addSuffix() throws Exception {
    // Create a temporary directory
    File tmpDir = getTempDir();
    Path tmpDirPath = tmpDir.toPath();

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
  }

  @Test
  public void testLocalSolrCoreAdmin_coreUsesCommonConfigSet() throws Exception {
    // Create LocalSolrCoreAdmin that does NOT use a common configuration set
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin(TEST_SOLR_CORE_NAME, -1);

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
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin;
    // TODO: Input to create() is its own LocalSolrCoreAdmin, so bad input has to come during construction

    // Bad instance directory
    coreAdmin = new SolrArtifactIndexAdmin.LocalSolrCoreAdmin(
        "bad",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        -1
    );

    assertThrows(IllegalStateException.class, () -> coreAdmin.create());

    // Test creating cores by iterating over versions of the LOCKSS configuration set
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {
      // Solr core name for this iteration
      String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      // Create a LocalSolrCoreAdmin instance with bare minimum parameters
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin(solrCoreName, version);

      // Assert the core does not exist yet
      assertCoreDoesNotExist(solrCoreName);

      // Create Solr core
      admin.create();

      // Assert the core exists and has the expected version
      assertLockssConfigSetVersion(solrCoreName, version);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_createCoreWithLatest() throws Exception {
    // Assert the core does not exists yet
    assertCoreDoesNotExist(TEST_SOLR_CORE_NAME);

    // Create core
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, TEST_SOLR_CORE_NAME);

    // Assert the core exists and has the expected version
    assertLockssConfigSetVersion(TEST_SOLR_CORE_NAME, LATEST_LOCKSS_CONFIGSET_VERSION);

    try {
      // Attempt to create the core again and assert a SolrException is thrown for the expected reason
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, TEST_SOLR_CORE_NAME);
      fail("Expected SolrException to be thrown");
    } catch (SolrException e) {
      if (e.getMessage().indexOf("core is already defined") == -1) {
        // SolrException was thrown for some reason other than the expected one - fail the test
        fail("Unexpected SolrException: {}", e);
      }
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_createCoreWithVersion() throws Exception {
    // Assert that negative version number throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, TEST_SOLR_CORE_NAME, -1)
    );

    // Iterate over versions of LOCKSS configuration set to install
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {
      // Solr core name for this iteration
      String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      // Sanity check: Assert a core by that name does not already exist
      assertCoreDoesNotExist(solrCoreName);

      // Create the core
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin.createCore(solrHomePath, solrCoreName, version);

      // Assert the core exists and has the expected LOCKSS configuration set version
      assertLockssConfigSetVersion(solrCoreName, version);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_fromSolrCore() throws Exception {
    // Assert passing a null SolrCore results in an IllegalArgumentException being thrown
    assertThrows(IllegalArgumentException.class, () -> SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrCore(null));

    // Create a container of all cores under the test Solr home base directory
    CoreContainer container = CoreContainer.createAndLoad(solrHomePath);

    // Build map from Solr core name to its SolrCore object
    Map<String, SolrCore> cores = container.getCores().stream().collect(
        Collectors.toMap(
            core -> core.getName(),
            core -> core
        )
    );

    for (PackagedTestCore pCore : PackagedTestCore.values()) {
      // SolrCore object for this packaged test core
      SolrCore core = cores.get(pCore.getCoreName());

      assertNotNull(core);

      // Assert that the LocalSolrCoreAdmin created by fromSolrCore(SolrCore) matches the expected LocalSolrCoreAdmin
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin expected = expected_fromSolrCore(core);
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin actual = SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrCore(core);
      assertEquals(expected, actual);
    }

    container.shutdown();
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
    for (PackagedTestCore pCore : PackagedTestCore.values()) {

      // Get a packaged Solr core admin
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = SolrArtifactIndexAdmin.LocalSolrCoreAdmin.fromSolrHomeAndCoreName(
          solrHomePath, pCore.getCoreName()
      );

      // Assert we got the same core admin
      assertEquals(expected_fromSolrHomeAndCoreName(solrHomePath, pCore.getCoreName()), admin);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_getLatestFileNameSuffix() throws Exception {
    // Create a temporary directory in which to perform the test
    File tmpDir = getTempDir();
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
  }

  /**
   * Test for LocalSolrCoreAdmin#getLockssConfigSetVersion() and LocalSolrCoreAdmin#getLockssConfigSetVersion(Path).
   *
   * @throws Exception
   */
  @Test
  public void testLocalSolrCoreAdmin_getLockssConfigSetVersion() throws Exception {
    // Version of LOCKSS configuration set to install
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {

      // Solr core name
      String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      // Create a LocalSolrCoreAdmin instance with bare minimum parameters
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin(solrCoreName, version);

      // Assert core does not exist yet
      assertCoreDoesNotExist(TEST_SOLR_CORE_NAME);

      // Create the Solr core
      admin.create();

      // Assert the installed LOCKSS configuration set version is equal to expected version (via JSON)
      assertEquals(version, admin.getLockssConfigSetVersion());

      // Assert the installed LOCKSS configuration set version is equal to expected version (via JSON, explict path)
      assertEquals(version, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersionFromOverlay(admin.configDirPath));

      // Assert the installed LOCKSS configuration set version is equal to expected version (via SolrCore)
      CoreContainer container = CoreContainer.createAndLoad(solrHomePath);
      SolrCore core = container.getCore(admin.solrCoreName);
      assertEquals(version, SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersionFromSolrCore(core));
      container.shutdown();
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_getLockssConfigSetVersionFromField() throws Exception {
    for (PackagedTestCore core : PackagedTestCore.values()) {
      int version = SolrArtifactIndexAdmin.LocalSolrCoreAdmin.getLockssConfigSetVersionFromField(
          solrHomePath,
          core.getCoreName()
      );

      log.trace("core = {}", core.getCoreName());
      log.trace("version = {}", version);

//      assertEquals(core.getConfigSetVersion(), version);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_getSegmentInfos() throws Exception {
    for (PackagedTestCore pCore : PackagedTestCore.values()) {
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = adminFromPackagedCore(pCore);

      SegmentInfos segInfos = coreAdmin.getSegmentInfos();
      assertNotNull(segInfos);

      if (pCore.isPopulated() || pCore.isSampled()) {
        assertTrue(segInfos.size() > 0);
        assertNotNull(segInfos.getMinSegmentLuceneVersion());
        assertMinSegmentLuceneVersion(pCore, segInfos.getMinSegmentLuceneVersion());
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
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = createTestLocalSolrCoreAdmin(coreName, -1);

      // Install LOCKSS configuration set version into core
      coreAdmin.installLockssConfigSetVersion(version);

      assertEquals(version, coreAdmin.getLockssConfigSetVersion());
      assertLockssConfigSet(coreAdmin.configDirPath, version);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_isCoreExists() throws Exception {
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin("test", 1);
    assertFalse(admin.isCoreExists());
    admin.create();
    assertTrue(admin.isCoreExists());
  }

  @Test
  public void testLocalSolrCoreAdmin_isLuceneIndexUpgradeAvailable() throws Exception {
    // Create new core; demonstrate it does not need an upgrade
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin1 =
        createTestLocalSolrCoreAdmin("test1", 1);

    assertThrows(IndexNotFoundException.class, () -> admin1.isLuceneIndexUpgradeAvailable());
    admin1.create();

    // No segments written yet; nothing to upgrade
    assertFalse(admin1.isLuceneIndexUpgradeAvailable());

    ArtifactSpec spec = ArtifactSpec.forCollAuUrl("c", "a", "u")
        .setArtifactId("ok")
        .setCollectionDate(0)
        .setStorageUrl("url")
        .generateContent()
        .thenCommit();

    // Add an artifact to the core, which will cause a segment to be written
    try (SolrClient solrClient = startEmbeddedSolrServer(solrHomePath, admin1.getCoreName())) {
      indexArtifactBean(solrClient, getArtifactBean(admin1.getLockssConfigSetVersion(), spec));
    }

    // Assert still no Lucene index upgrade available since latest version used
    assertFalse(admin1.isLuceneIndexUpgradeAvailable());
    assertEquals(SolrArtifactIndexAdmin.TARGET_LUCENE_VERSION, admin1.getSegmentInfos().getMinSegmentLuceneVersion());

    // Demonstrate packaged Solr 6.x cores have a Lucene index upgrade available
    for (PackagedTestCore core : PackagedTestCore.getCoresWithMajorVersion(6)) {
      if (core.isPopulated() || core.isSampled()) {
        assertTrue(adminFromPackagedCore(core).isLuceneIndexUpgradeAvailable());
      }
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_isLockssConfigSetUpdateAvailable() throws Exception {
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin1 =
        createTestLocalSolrCoreAdmin("test1", 1);

    // Core doesn't exist yet so a LOCKSS configuration set is "always available"
    assertTrue(admin1.isLockssConfigSetUpdateAvailable());

    admin1.create();
    assertTrue(admin1.isLockssConfigSetUpdateAvailable());
    admin1.updateConfigSet();
    assertFalse(admin1.isLockssConfigSetUpdateAvailable());

    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin2 =
        createTestLocalSolrCoreAdmin("test2", LATEST_LOCKSS_CONFIGSET_VERSION);

    // Core doesn't exist yet so a LOCKSS configuration set is "always available"
    assertTrue(admin2.isLockssConfigSetUpdateAvailable());

    admin2.create();
    assertFalse(admin2.isLockssConfigSetUpdateAvailable());
    admin2.updateConfigSet();
    assertFalse(admin2.isLockssConfigSetUpdateAvailable());
  }

  @Test
  public void testLocalSolrCoreAdmin_isReindexInProgress() throws Exception {
    // Create a new Solr core
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin("test", 1);
    admin.create();

    File reindexLockFile = admin.indexDir.resolve(REINDEX_LOCK_FILE).toFile();

    // Assert no reindex in progress
    assertFalse(admin.isReindexInProgress());

    // Simulate a reindex lock acquire
    FileUtils.touch(reindexLockFile);

    // Assert reindex is in progress
    assertTrue(admin.isReindexInProgress());

    // Simulate a reindex lock release
    FileUtils.forceDelete(reindexLockFile);

    // Assert no reindex in progress
    assertFalse(admin.isReindexInProgress());

    /*
    try (FileChannel channel = new RandomAccessFile(reindexLockFile, "rw").getChannel()) {
      try (FileLock lock = channel.tryLock()) {
        // Assert reindex is in progress
        assertTrue(admin.isReindexInProgress());
      } finally {
        // Assert reindex is no longer in progress (i.e., reindex finished)
        assertFalse(admin.isReindexInProgress());
      }
    }
    */
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
    File tmpDir = getTempDir();
    Path tmpDirPath = tmpDir.toPath();

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
  }

  /**
   * Test for {@code SolrArtifactIndexAdmin.LocalSolrCoreAdmin#update()}.
   * <p>
   * This test exercises the cumulative action of {@code updateLuceneIndex()} and {@code updateConfigSet()}.
   *
   * @throws Exception
   */
  @Test
  public void testLocalSolrCoreAdmin_updateFromEmpty() throws Exception {
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {
      // Solr core name
      String solrCoreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin(solrCoreName, version);

      // Assert the core does not exist yet
      assertCoreDoesNotExist(solrCoreName);

      // Create the Solr core with LOCKSS config set version 1
      admin.create();

      // Get the LOCKSS configuration set version and assert it is latest
      assertEquals(version, admin.getLockssConfigSetVersion());

      // Perform update()
      admin.update();

      // Get the LOCKSS configuration set version and assert it is latest
      assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, admin.getLockssConfigSetVersion());

      // Assert null minimum segment version because the index is empty / there are no segments yet
      assertNull(admin.getSegmentInfos().getMinSegmentLuceneVersion());

      ArtifactSpec spec = ArtifactSpec.forCollAuUrl("c", "a", "u")
          .setArtifactId("ok")
          .setCollectionDate(0)
          .setStorageUrl("url")
          .generateContent()
          .thenCommit();

      // Add an artifact to the core
      try (SolrClient solrClient = startEmbeddedSolrServer(solrHomePath, solrCoreName)) {
        indexArtifactBean(solrClient, getArtifactBean(admin.getLockssConfigSetVersion(), spec));
      }

      // Get segment information from index and assert the minimum version is on or after the expected version (latest)
      SegmentInfos segInfos = admin.getSegmentInfos();
      assertNotNull(segInfos);
      assertTrue(segInfos.getMinSegmentLuceneVersion().onOrAfter(LATEST_LUCENE_VERSION));
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_updateConfigSetNewCore() throws Exception {
    for (int version = 1; version <= LATEST_LOCKSS_CONFIGSET_VERSION; version++) {

      // Solr core name for this iteration
      String coreName = String.format("%s-%d", TEST_SOLR_CORE_NAME, version);

      // Create a LocalSolrCoreAdmin for testing
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = createTestLocalSolrCoreAdmin(coreName, version);

      // Assert the core does not exist yet
      assertCoreDoesNotExist(coreName);

      // Create the core
      coreAdmin.create();

      // Assert the LOCKSS configuration set version installed is what we created the core with
      assertEquals(version, coreAdmin.getLockssConfigSetVersion());

      ArtifactSpec spec = ArtifactSpec.forCollAuUrl("c", "a", "u")
          .setArtifactId("ok")
          .setCollectionDate(0)
          .setStorageUrl("url")
          .generateContent()
          .thenCommit();

      // Add an artifact to the core
      try (SolrClient solrClient = startEmbeddedSolrServer(solrHomePath, coreName)) {
        indexArtifactBean(solrClient, getArtifactBean(version, spec));
      }

      // Update the configuration set
      coreAdmin.updateConfigSet();

      // Assert the installed LOCKSS configuration set is the latest version
      assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, coreAdmin.getLockssConfigSetVersion());
      assertLockssConfigSet(coreAdmin.configDirPath, LATEST_LOCKSS_CONFIGSET_VERSION);
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_updateConfigSet() throws Exception {
    // Iterate over packaged test cores
    for (PackagedTestCore pCore : PackagedTestCore.values()) {

      // Assert set of packaged artifacts is reflected core (for generated cores)
      if (pCore.isPopulated()) {
        assertPackagedArtifacts(pCore);
      }

      // Get LocalSolrCoreAdmin of packaged test core
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = adminFromPackagedCore(pCore);
      assertNotNull(coreAdmin);

      // Assert current configuration set version matches expected
      assertEquals(pCore.getConfigSetVersion(), coreAdmin.getLockssConfigSetVersion());

      // Upgrade the core's Lucene index
      coreAdmin.updateConfigSet();

      // Assert configuration set version matches latest
      assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, coreAdmin.getLockssConfigSetVersion());

      // Assert set of packaged artifacts is still reflected core (for generated cores)
      if (pCore.isPopulated()) {
        assertPackagedArtifacts(pCore, pCore.getArtifactClass(LATEST_LOCKSS_CONFIGSET_VERSION));
      }
    }
  }

  @Test
  public void testLocalSolrCoreAdmin_updateLuceneIndex() throws Exception {
    // Iterate over packaged test cores
    for (PackagedTestCore pCore : PackagedTestCore.values()) {

      // Assert set of packaged artifacts is reflected core
      assertPackagedArtifacts(pCore);

      // Assert expected Lucene index version for this packaged core
      assertMinSegmentLuceneVersion(pCore, pCore.getSolrVersion());

      // Upgrade the core's Lucene index
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin coreAdmin = adminFromPackagedCore(pCore);
      assertNotNull(coreAdmin);
      coreAdmin.updateLuceneIndex();

      // Assert Lucene index is at latest version
      assertMinSegmentLuceneVersion(pCore, LATEST_LUCENE_VERSION);

      // Assert set of packaged artifacts is still reflected core
      assertPackagedArtifacts(pCore);
    }
  }

  // TESTS: main(...) //////////////////////////////////////////////////////////////////////////////////////////////////

  @Test
  @ExpectSystemExitWithStatus(0)
  public void testMain_createLocalCore() throws Exception {
    // Create a new Solr core
    String[] args = {"--action", "create", "--local", solrHomePath.toString(), "--core", "test"};
    SolrArtifactIndexAdmin.main(args);

    // Assert core exists and latest LOCKSS configuration set installed
    assertLockssConfigSetVersion("test", LATEST_LOCKSS_CONFIGSET_VERSION);
    assertLockssConfigSet(solrHomePath.resolve("lockss/cores/test/conf"), LATEST_LOCKSS_CONFIGSET_VERSION);
  }

  /**
   * Assert that calling create core on a core that already exists is a no-op.
   *
   * @throws Exception
   */
  @Test
  @ExpectSystemExitWithStatus(0)
  public void testMain_createLocalCore_createTwice() throws Exception {
    String[] args = {"--action", "create", "--local", solrHomePath.toString(), "--core", "test"};
    SolrArtifactIndexAdmin.main(args);
    SolrArtifactIndexAdmin.main(args);
  }

  @Test
  @ExpectSystemExitWithStatus(0)
  public void testMain_updateLocalCores() throws Exception {
    // Update existing Solr cores
    for (PackagedTestCore pCore : PackagedTestCore.values()) {
      String[] args = {"--action", "update", "--local", solrHomePath.toString(), "--core", pCore.getCoreName()};

      log.trace("args = {}", Arrays.asList(args));

      // Update the core through main()
      SolrArtifactIndexAdmin.main(args);

      // Assert update
      SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = adminFromPackagedCore(pCore);

      // Get the LOCKSS configuration set version and assert it is latest
      assertEquals(LATEST_LOCKSS_CONFIGSET_VERSION, admin.getLockssConfigSetVersion());
      assertLockssConfigSet(admin.configDirPath, LATEST_LOCKSS_CONFIGSET_VERSION);

      if (pCore.isPopulated() || pCore.isSampled()) {
        // Get segment information from index and assert the minimum version is on or after the latest version
        assertMinSegmentLuceneVersion(pCore, LATEST_LUCENE_VERSION);
      } else {
        // Assert null minimum segment version because the index is empty / there are no segments yet
        assertNull(admin.getSegmentInfos().getMinSegmentLuceneVersion());
      }
    }
  }

  @Test
  @ExpectSystemExitWithStatus(0)
  public void testMain_verifyLocalCore_success() throws Exception {
    String[] argsCreate = {"--action", "create", "--local", solrHomePath.toString(), "--core", "test"};
    String[] argsVerify = {"--action", "verify", "--local", solrHomePath.toString(), "--core", "test"};

    SolrArtifactIndexAdmin.main(argsCreate);
    SolrArtifactIndexAdmin.main(argsVerify);
  }

  @Test
  @ExpectSystemExitWithStatus(1)
  public void testMain_verifyLocalCore_coreMissing() throws Exception {
    String[] argsVerify = {"--action", "verify", "--local", solrHomePath.toString(), "--core", "test"};
    SolrArtifactIndexAdmin.main(argsVerify);
  }

  @Test
  @ExpectSystemExitWithStatus(2)
  public void testMain_verifyLocalCore_luceneIndexUpgradeNeeded() throws Exception {
    PackagedTestCore core = PackagedTestCore.SOLR6_POPULATED_V1;
    String[] argsVerify = {"--action", "verify", "--local", solrHomePath.toString(), "--core", core.getCoreName()};
    SolrArtifactIndexAdmin.main(argsVerify);
  }

  @Test
  @ExpectSystemExitWithStatus(2)
  public void testMain_verifyLocalCore_lockssConfigSetUpdateNeeded() throws Exception {
    PackagedTestCore core = PackagedTestCore.SOLR6_POPULATED_V1;
    String[] argsVerify = {"--action", "verify", "--local", solrHomePath.toString(), "--core", core.getCoreName()};
    SolrArtifactIndexAdmin.main(argsVerify);
  }

  @Test
  @ExpectSystemExitWithStatus(4)
  public void testMain_verifyLocalCore_updateInProgress() throws Exception {
    // Create a new Solr core
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin("test", 1);
    admin.create();

    // Lock files
    File reindexLockFile = admin.indexDir.resolve(REINDEX_LOCK_FILE).toFile();
    File upgradeLockFile = admin.indexDir.resolve(UPGRADE_LOCK_FILE).toFile();

    log.trace("reindexLockFile = {}", reindexLockFile);
    log.trace("upgradeLockFile = {}", upgradeLockFile);

    // Simulate an update lock
    try (FileChannel channel = new RandomAccessFile(upgradeLockFile, "rw").getChannel()) {
      try (FileLock updateLock = channel.tryLock()) {

        // Simulate a reindex lock
        FileUtils.touch(reindexLockFile);

        // Assert reindex is in progress within an update
        assertTrue(admin.isUpdateInProgress());
        assertTrue(admin.isReindexInProgress());

        // Call verify (which should call System.exit(4))
        String[] args = {"--action", "verify", "--local", solrHomePath.toString(), "--core", admin.getCoreName()};
        SolrArtifactIndexAdmin.main(args);
      }
    }
  }

  @Test
  @ExpectSystemExitWithStatus(8)
  public void testMain_verifyLocalCore_reindexInterrupted() throws Exception {
    // Create a new Solr core
    SolrArtifactIndexAdmin.LocalSolrCoreAdmin admin = createTestLocalSolrCoreAdmin("test", 1);
    admin.create();

    // Lock file
    File reindexLockFile = admin.indexDir.resolve(REINDEX_LOCK_FILE).toFile();

    // Assert no reindex in progress
    assertFalse(admin.isUpdateInProgress());
    assertFalse(admin.isReindexInProgress());

    // Simulate a reindex lock
    FileUtils.touch(reindexLockFile);

    // Assert reindex is in progress but not an update
    assertFalse(admin.isUpdateInProgress());
    assertTrue(admin.isReindexInProgress());

    try {
      // Call verify (which should call System.exit(1))
      String[] args = {"--action", "verify", "--local", solrHomePath.toString(), "--core", admin.getCoreName()};
      SolrArtifactIndexAdmin.main(args);
    } finally {
      // Assert reindex is still in progress (interrupted)
      assertFalse(admin.isUpdateInProgress());
      assertTrue(admin.isReindexInProgress());
    }
  }

  // TESTS: Common /////////////////////////////////////////////////////////////////////////////////////////////////////

  @Test
  public void testCoreExists() throws Exception {
    String coreName = PackagedTestCore.SOLR6_POPULATED_V1.getCoreName();

    try (SolrClient solrClient = startEmbeddedSolrServer(solrHomePath, coreName)) {
      assertTrue(SolrArtifactIndexAdmin.coreExists(solrClient, coreName));
      assertFalse(SolrArtifactIndexAdmin.coreExists(solrClient, "foo"));
    }
  }

}
