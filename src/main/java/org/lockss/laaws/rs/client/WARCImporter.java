/*

Copyright (c) 2017-2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */

package org.lockss.laaws.rs.client;

import org.apache.commons.cli.*;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.lockss.laaws.rs.core.LocalLockssRepository;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.log.L4JLogger;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * Imports a WARC file into the repository.
 */
public class WARCImporter {
  private static final L4JLogger log = L4JLogger.getLogger();
  private LockssRepository repository;
  private String collection;
  private String auid;

  /**
   * Constructor when using an existing repository.
   * 
   * @param repository
   *          A LockssRepository with the existing repository where to import
   *          the WARC file.
   * @param collection
   *          A String with the name of the collection where to import the WARC
   *          file.
   * @param auid
   *          A String with the Archival Unit identifier linked to the imported
   *          WARC file.
   */
  public WARCImporter(LockssRepository repository, String collection,
      String auid) {
    this.repository = repository;
    this.collection = collection;
    this.auid = auid;
  }

  /**
   * Constructor when using a new local repository.
   * 
   * @param repoDir
   *          A File with the directory of the local repository to be used.
   * @param persistedIndexName
   *          A String with the name of the file where to persist the repository
   *          index.
   * @param collection
   *          A String with the name of the collection where to import the WARC
   *          file.
   * @param auid
   *          A String with the Archival Unit identifier linked to the imported
   *          WARC file.
   * @throws IOException
   *           if there are problems creating the local repository.
   */
  public WARCImporter(File repoDir, String persistedIndexName,
      String collection, String auid) throws IOException {
    this(new LocalLockssRepository(repoDir, persistedIndexName), collection,
	auid);
  }

  /**
   * Constructor when using a new REST service repository.
   * 
   * @param url
   *          A URL with the location of the repository REST service to be used.
   * @param collection
   *          A String with the name of the collection where to import the WARC
   *          file.
   * @param auid
   *          A String with the Archival Unit identifier linked to the imported
   *          WARC file.
   */
  public WARCImporter(URL url, String collection, String auid) {
    this(new RestLockssRepository(url), collection, auid);
  }

  /**
   * Main entry as a standalone program.
   * 
   * @param args
   *          A String[] with the command line arguments.
   * @throws ParseException
   * @throws MalformedURLException
   * @throws IOException
   */
  public static void main(String[] args)
      throws ParseException, MalformedURLException, IOException {
    log.debug2("args: {}", () -> Arrays.asList(args));

    // Setup command line options
    Options options = new Options();
    options.addOption("l", "localRepository", true,
	"Target local repository URL");
    options.addOption("r", "restRepository", true, "Target repository URL");
    options.addOption("c", "collection", true, "Target collection ID");
    options.addOption("a", "auid", true, "Archival Unit ID (AUID)");

    // Parse options
    CommandLine cmd = new DefaultParser().parse(options, args);

    // Get the specified identifier of the Archival Unit to be linked to the
    // imported artifacts, if any.
    String auid = null;

    if (cmd.hasOption("auid")) {
      auid = cmd.getOptionValue("auid");
      log.trace("Forcing AUID of WARC records to: {}", auid);
    }

    // Get the specified collection where to import the WARC file.
    String collection = cmd.getOptionValue("collection");
    log.trace("collection: {}", collection);

    WARCImporter warcImporter = null;

    // Check whether a REST service repository has been specified.
    if (cmd.hasOption("restRepository")) {
      // Yes: Get the REST service repository URL specification.
      String restServiceUrlSpec = cmd.getOptionValue("restRepository");
      log.trace("Using the REST Service at: {}", restServiceUrlSpec);

      // Create the WARC file importer.
      warcImporter =
	  new WARCImporter(new URL(restServiceUrlSpec), collection, auid);
      // No: Check whether a local repository has been specified.
    } else if (cmd.hasOption("localRepository")) {
      // Yes: Get the local repository directory.
      String localRepoDir = cmd.getOptionValue("localRepository");
      log.trace("Using the local directory: {}", localRepoDir);

      // Create the WARC file importer.
      warcImporter = new WARCImporter(new File(localRepoDir),
	  "artifact-index.ser", collection, auid);
    } else {
      // No: Report the error.
      log.error("No repository data found:"
	  + " Either a -l or a -r option must be specified");
      System.exit(1);
    }

    // Treat the remaining arguments as WARC file paths
    List<String> warcs = cmd.getArgList();
    log.trace("warcs: {}", () -> warcs);

    // Iterate over WARCs and import
    for (String warc : warcs) {
      log.trace("warc: {}", warc);

      try {
	warcImporter.importWARC(new File(warc));
      } catch (IOException ioe) {
	log.error("importWARC failed", ioe);
      }
    }
  }

  /**
   * Imports into the repository a WARC file.
   * 
   * @param warc
   *          A File with the specification of the WARC file to be imported.
   * @return a LockssRepository with a handle to the repository where the WARC
   *         file has been imported.
   * @throws IOException
   *           if there are problems importing the WARC file.
   */
  public LockssRepository importWARC(File warc) throws IOException {
    log.debug2("warc: {}", () -> warc);
    log.debug2("collection: {}", collection);
    log.debug2("auid: {}", auid);

    int processedCount = 0;
    int importedCount = 0;
    int ignoredCount = 0;

    // Get a WARCReader for this WARC file and iterate through its records:
    for (ArchiveRecord record : WARCReaderFactory.get(warc)) {
      ArchiveRecordHeader headers = record.getHeader();
      String recordType =
	  (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

      log.trace(String.format(
	  "Importing WARC record (ID: %s, type: %s), headers: %s",
	  record.getHeader().getHeaderValue("WARC-Record-ID"),
	  recordType,
	  record.getHeader()
	  ));

      // Convert ArchiveRecord to ArtifactData
      ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);
      log.trace("artifactData: {}", () -> artifactData);

      // Upload artifact
      if (artifactData != null) {
	Integer version = -1;
	String versionHeader = headers.getVersion();
	log.trace("versionHeader: {}", versionHeader);

	if ((versionHeader != null) && (!versionHeader.isEmpty())) {
	  version = Integer.valueOf(versionHeader);
	}

	log.trace("version: {}", version);

	// Create an ArtifactIdentifier
	ArtifactIdentifier identifier = new ArtifactIdentifier(
	    collection,
	    auid,
	    headers.getUrl(),
	    version
	    );

	log.trace("identifier: {}", () -> identifier);

	// Set the artifact identifier
	artifactData.setIdentifier(identifier);

	// Upload the artifact
	Artifact artifact = repository.addArtifact(artifactData);
	log.debug("Uploaded artifact: {}", () -> artifact);

	String artifactId = artifact.getId();

	// Commit artifact immediately
	repository.commitArtifact(collection, artifactId);
	log.info("Committed artifactId: {}", artifactId);

	importedCount++;
      } else {
	log.debug("WARC record {} ignored", () -> headers);
	ignoredCount++;
      }

      processedCount++;
    }

    log.info(String.format("WARC File %s processing completed: %d processed, "
	+ "%d imported, %d ignored.", warc, processedCount, importedCount,
	ignoredCount));

    return repository;
  }

  /**
   * Provides the repository used by this object.
   * 
   * @return a LockssRepository with the repository used by this object.
   */
  public LockssRepository getRepository() {
    return repository;
  }
}
