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

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.model.ArtifactData;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

public class WARCImporter {
    private static final Log log = LogFactory.getLog(WARCImporter.class);
    private static RestLockssRepository repo;

    public static void importWARC(File warc, String collection, String auid) throws IOException, HttpException {
        List<String> artifactIds = new LinkedList<>();

        // Get a WARCReader for this WARC file and iterate through its records:
        for (ArchiveRecord record : WARCReaderFactory.get(warc)) {
            ArchiveRecordHeader headers = record.getHeader();
            String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

            log.info(String.format(
                    "Importing WARC record (ID: %s, type: %s), headers: %s",
                    record.getHeader().getHeaderValue("WARC-Record-ID"),
                    recordType,
                    record.getHeader()
            ));

            // Convert ArchiveRecord to ArtifactData
            ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

            // Upload artifact
            if (artifactData != null) {
                // Create an ArtifactIdentifier
                ArtifactIdentifier identifier = new ArtifactIdentifier(
                        collection,
                        auid,
                        headers.getUrl(),
                        headers.getVersion()
                );

                // Set the artifact identifier
                artifactData.setIdentifier(identifier);

                // Upload the artifact
                Artifact artifact = repo.addArtifact(artifactData);
                String artifactId = artifact.getId();

                // Commit artifact immediately
                repo.commitArtifact(collection, artifactId);

                // Track artifact IDs
                artifactIds.add(artifactId);
            }

            /*
            // Logging and debugging
            if (artifactIndex != null) {
                log.debug(String.format("Successfully added WARC record %s to repository: %s",
                        headers.getRecordIdentifier(),
                        artifactIndex));
            } else {
                //log.warn(String.format("Could not add "));
            }
            */
        }

        // Debugging
        for (String artifactId : artifactIds) {
            // Get HTTP status
            //ArtifactData artifact = repo.getArtifact(collection, artifactId);
            //log.debug(artifact.getHttpStatus());
        }
    }


    public static void main(String[] args) throws ParseException, MalformedURLException {

        // Setup command line options
        Options options = new Options();
        options.addOption("r", "repository", true, "Target repository URL");
        options.addOption("c", "collection", true, "Target collection ID");
        options.addOption("a", "auid", true, "Archival Unit ID (AUID)");

        // Parse options
        CommandLine cmd = new PosixParser().parse(options, args);
        if (cmd.hasOption("auid")) {
            log.info("Forcing AUID of WARC records to: " + cmd.getOptionValue("auid"));
        }

        // Create a handle to the LOCKSS repository service
        repo = new RestLockssRepository(new URL(cmd.getOptionValue("repository")));

        // Treat the remaining arguments as WARC file paths
        List<String> warcs = cmd.getArgList();

        // Iterate over WARCs and import
        for (String warc : warcs) {
            try {
                importWARC(new File(warc), cmd.getOptionValue("collection"), cmd.getOptionValue("auid"));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (HttpException e) {
                e.printStackTrace();
            }
        }
    }

}
