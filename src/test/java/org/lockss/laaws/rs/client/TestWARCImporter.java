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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_IP;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.archive.format.warc.WARCConstants;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.UTF8Bytes;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.archive.io.warc.WARCWriterPoolSettings;
import org.archive.io.warc.WARCWriterPoolSettingsData;
import org.archive.uid.UUIDGenerator;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.ANVLRecord;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.core.AbstractLockssRepositoryTest.ArtSpec;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for org.lockss.laaws.rs.client.WARCImporter.
 */
public class TestWARCImporter extends LockssTestCase5 {
  private static final L4JLogger log = L4JLogger.getLogger();

  @Test
  public void test() throws Exception {
    // Expectations of the artifact created.
    ArtSpec artSpec = ArtSpec.forCollAuUrlVer("collection1", "auid1",
	"http://some.server.org/", 1);

    artSpec.setStatusLine(new BasicStatusLine(new ProtocolVersion("HTTP", 1,1),
	200, "OK"));

    String body =  "<!DOCTYPE html>\n"
	+ "<html>\n"
	+ "<head>\n"
	+ "<title>Test WARC Importer 1</title>\n"
	+ "<meta charset=\"utf-8\">\n"
	+ "</head>\n"
	+ "<body>\n"
	+ "</body>\n"
	+ "</html>\n";

    artSpec.setContent(body);
    artSpec.setContentLength(body.length());

    Map<String, String> headers = new HashMap<>();
    headers.put("Date", "Mon, 28 Dec 1988 03:36:21 GMT");
    headers.put("Content-Length", String.valueOf(artSpec.getContentLength()));
    headers.put("Content-Type", "text/html");

    artSpec.setHeaders(headers);

    // The directory where the generated test WARC file is created.
    File warcDir = getTempDir();

    String storageUrl = "file://" + warcDir.getAbsolutePath()
    + "/collections/" + artSpec.getCollection()
    + "/au-116cf2bbfdcfbe0c9ad94987b00101cd/artifacts.warc?offset=0";

    artSpec.setStorageUrl(storageUrl);

    // Generate the test WARC file.
    File warc1 = generateWarcFile("generatedWarc1", warcDir, artSpec);

    // Import into the repository the test WARC file just generated.
    WARCImporter warcImporter = new WARCImporter(warcDir, "artifact-index.ser",
	artSpec.getCollection(), artSpec.getAuid());
    warcImporter.importWARC(warc1);

    // Get the repository used by the WARC import process.
    LockssRepository repository = warcImporter.getRepository();

    int count = 0;

    // Loop through all the collections existing in the repository.
    for (String collection : repository.getCollectionIds()) {
      // Verify the single collection.
      assertEquals(1, ++count);
      assertEquals(artSpec.getCollection(), collection);

      // Loop through all the Archival Units linked to this collection.
      for (String auId : repository.getAuIds(collection)) {
	// Verify the single Archival Unit.
	assertEquals(0, --count);
	assertEquals(artSpec.getAuid(), auId);

	// Loop through all the Artifacts linked to this Archival Unit and
	// collection pair.
	for (Artifact artifact
	    : repository.getAllArtifactsAllVersions(collection, auId)) {
	  // Verify the single Artifact.
	  assertEquals(1, ++count);

	  // Verify the artifact contents.
	  assertEquals(collection, artifact.getCollection());
	  assertEquals(auId, artifact.getAuid());
	  assertTrue(artifact.getCommitted());
	  artSpec.assertData(repository, artifact);
	}
      }
    }
  }

  /**
   * Generates a WARC file.
   * 
   * @param filename
   *          A String with the name of the WARC file to be used.
   * @param warcDir
   *          A File with the directory where to create the WARC file.
   * @param artSpec
   *          An ArtSpec with all the information needed to create and store an
   *          Artifact.
   * @return a File with the specification of the generated WARC file.
   * @throws IOException
   *           if there are problems creating the WARC file.
   */
  private File generateWarcFile(String filename, File warcDir, ArtSpec artSpec)
      throws IOException {
    log.debug2("filename = " + filename);
    log.debug2("warcDir = " + warcDir);

    List<File> outputDirs = new ArrayList<>();
    outputDirs.add(warcDir);

    List<String> metadata = new ArrayList<String>();
    metadata.add("mdKey1: mdValue1\r\nmdKey2: mdValue2\r\n");

    WARCWriterPoolSettings settings = new WARCWriterPoolSettingsData("",
	filename, -1, false, outputDirs, metadata, new UUIDGenerator());

    WARCWriter writer = new WARCWriter(new AtomicInteger(), settings);

    try {
      writeWarcinfoRecord(settings, writer);
      writeMetadataRecords(writer);
      writeRequestRecord(writer);
      writeResponseRecord(writer, artSpec);
    } finally {
      writer.close();
    }

    File generatedFile = new File(warcDir, filename + ".warc");
    if (log.isDebugEnabled()) log.debug("generatedFile = " + generatedFile);
    return generatedFile;
  }

  /**
   * 
   * @param settings
   * @param writer
   * @throws IOException
   */
  private void writeWarcinfoRecord(WARCWriterPoolSettings settings,
      WARCWriter writer) throws IOException {
    if (log.isDebugEnabled()) log.debug("Invoked");

    WARCRecordInfo recordInfo = new WARCRecordInfo();
    recordInfo.setType(WARCRecordType.warcinfo);
    recordInfo.setUrl("http://www.lockss.org/");
    recordInfo.setCreate14DigitDate(ArchiveUtils.getLog14Date());
    recordInfo.setMimetype(ANVLRecord.MIMETYPE);
    recordInfo.setExtraHeaders(null);
    recordInfo.setEnforceLength(true);
    
    ANVLRecord content = new ANVLRecord();
    content.addLabelValue("contentLabel1", "contentValue1");
    content.addLabelValue("contentLabel2", "contentValue2");
    content.addLabelValue("contentLabel3", "contentValue3");

    byte [] bytes = content.getUTF8Bytes();
    recordInfo.setContentStream(new ByteArrayInputStream(bytes));
    recordInfo.setContentLength((long) bytes.length);
	
    URI recordid = settings.getRecordIDGenerator()
	.getQualifiedRecordID(WARCWriter.TYPE,
	    WARCRecordType.warcinfo.toString());

    recordInfo.setRecordId(recordid);

    writer.writeRecord(recordInfo);
    if (log.isDebugEnabled()) log.debug("Done");
  }

  /**
   * 
   * @param writer
   * @throws IOException
   */
  private void writeMetadataRecords(final WARCWriter writer)
      throws IOException {
    if (log.isDebugEnabled()) log.debug("Invoked");

    WARCRecordInfo recordInfo = new WARCRecordInfo();
    recordInfo.setType(WARCRecordType.metadata);
    recordInfo.setUrl("http://www.laaws.lockss.org/");
    recordInfo.setCreate14DigitDate(ArchiveUtils.get14DigitDate());
    recordInfo.setMimetype("text/plain");
    recordInfo.setEnforceLength(true);

    ANVLRecord headerFields = new ANVLRecord();
    headerFields.addLabelValue("headerName1", "headerValue1");
    headerFields.addLabelValue("headerName2", "headerValue2");
    recordInfo.setExtraHeaders(headerFields);

    URI rid = (new UUIDGenerator()).getQualifiedRecordID("type",
	WARCRecordType.metadata.toString());
    recordInfo.setRecordId(rid);

    for (int i = 1; i <= 10; i++) {
      String body = "Body line " + i;
      byte [] bodyBytes = body.getBytes(UTF8Bytes.UTF8);
      recordInfo.setContentStream(new ByteArrayInputStream(bodyBytes));
      recordInfo.setContentLength((long)bodyBytes.length);
      writer.writeRecord(recordInfo);
    }

    if (log.isDebugEnabled()) log.debug("Done");
  }

  /**
   * Writes a request record to a WARC file.
   * 
   * @param writer
   *          A WARCWriter used to write content to the WARC file.
   * @throws IOException
   *           if there are problems writing the WARC file.
   */
  private void writeRequestRecord(final WARCWriter writer)
      throws IOException {
    if (log.isDebugEnabled()) log.debug("Invoked");

    WARCRecordInfo recordInfo = new WARCRecordInfo();
    recordInfo.setType(WARCRecordType.request);
    recordInfo.setUrl("http://www.laaws.lockss.org/robots.txt");
    recordInfo.setCreate14DigitDate(ArchiveUtils.get14DigitDate());
    recordInfo.setMimetype(WARCConstants.HTTP_REQUEST_MIMETYPE);
    recordInfo.setEnforceLength(true);

    ANVLRecord headers = new ANVLRecord();
    headers.addLabelValue(HEADER_KEY_IP, "192.168.1.1");
    recordInfo.setExtraHeaders(headers);

    URI rid = (new UUIDGenerator()).getQualifiedRecordID("type",
	WARCRecordType.request.toString());
    recordInfo.setRecordId(rid);

    String content = "GET /robots.txt HTTP/1.1\n"
	+ "User-Agent: Wget/1.14 (linux-gnu)\n"
	+ "Accept: */*\n"
	+ "Host: www.laaws.lockss.org\n"
	+ "Connection: Keep-Alive\n";

    byte [] contentbytes = content.getBytes(UTF8Bytes.UTF8);
    recordInfo.setContentStream(new ByteArrayInputStream(contentbytes));
    recordInfo.setContentLength((long)contentbytes.length);
    writer.writeRecord(recordInfo);

    if (log.isDebugEnabled()) log.debug("Done");
  }

  /**
   * Writes a response record to a WARC file.
   * 
   * @param writer
   *          A WARCWriter used to write content to the WARC file.
   * @param artSpec
   *          An ArtSpec with all the information needed to create and store an
   *          Artifact.
   * @throws IOException
   *           if there are problems writing the WARC file.
   */
  private void writeResponseRecord(final WARCWriter writer, ArtSpec artSpec)
      throws IOException {
    if (log.isDebugEnabled()) log.debug("Invoked");

    WARCRecordInfo recordInfo = new WARCRecordInfo();
    recordInfo.setType(WARCRecordType.response);
    recordInfo.setUrl(artSpec.getUrl());
    recordInfo.setCreate14DigitDate(ArchiveUtils.get14DigitDate());
    recordInfo.setMimetype(WARCConstants.HTTP_RESPONSE_MIMETYPE);
    recordInfo.setEnforceLength(true);

    ANVLRecord headers = new ANVLRecord();
    headers.addLabelValue(HEADER_KEY_IP, "127.0.0.1");
    recordInfo.setExtraHeaders(headers);

    URI rid = (new UUIDGenerator()).getQualifiedRecordID("type",
	WARCRecordType.response.toString());
    recordInfo.setRecordId(rid);

    String content = artSpec.getStatusLine() + "\n"
	+ artSpec.getHeadersAsText() + "\n"
	+ artSpec.getContent();

    byte [] contentbytes = content.getBytes(UTF8Bytes.UTF8);
    recordInfo.setContentStream(new ByteArrayInputStream(contentbytes));
    recordInfo.setContentLength((long)contentbytes.length);
    writer.writeRecord(recordInfo);

    if (log.isDebugEnabled()) log.debug("Done");
  }
}