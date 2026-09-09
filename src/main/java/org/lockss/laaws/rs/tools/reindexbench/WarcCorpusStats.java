/*

Copyright (c) 2017-2019 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.tools.reindexbench;

import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Cheap, bounded-cost WARC inspection utilities shared by {@link ManifestHelper}.
 * <p>
 * Nothing here ever reads a whole large WARC end-to-end just to describe it: per the WARC-parse
 * finding this benchmark's design is based on, advancing a compressed WARC reader past a record
 * forces decompression of that record's full payload regardless of whether anything reads it -
 * so "read every record just to count them" costs nearly as much as the actual benchmark. The
 * sampling method here reads at most the first {@code sampleSize} records of a file and then
 * stops, never iterating to the end.
 */
public class WarcCorpusStats {

  /** Result of sampling the first N records of a WARC file to estimate its record population. */
  public static class SampleResult {
    public final long sizeBytes;
    public final boolean compressed;
    public final int sampledRecordCount;
    /** -1 if fewer than 2 records were sampled (not enough data to estimate an average). */
    public final double estimatedAvgRecordBytes;
    /** -1 if {@link #estimatedAvgRecordBytes} could not be computed. */
    public final long estimatedTotalRecords;

    SampleResult(long sizeBytes, boolean compressed, int sampledRecordCount,
                 double estimatedAvgRecordBytes, long estimatedTotalRecords) {
      this.sizeBytes = sizeBytes;
      this.compressed = compressed;
      this.sampledRecordCount = sampledRecordCount;
      this.estimatedAvgRecordBytes = estimatedAvgRecordBytes;
      this.estimatedTotalRecords = estimatedTotalRecords;
    }
  }

  /**
   * Samples at most the first {@code sampleSize} records of a WARC file to cheaply estimate its
   * average record size (in on-disk bytes, i.e. compressed size for a {@code .warc.gz}) and,
   * from that, its total record count. Stops reading after the sample regardless of file size.
   *
   * @param warcFile   Path to the WARC file.
   * @param compressed Whether the file is gzip-compressed (typically decided by filename).
   * @param sampleSize Maximum number of records to read from the start of the file.
   */
  public static SampleResult sample(Path warcFile, boolean compressed, int sampleSize) throws IOException {
    long sizeBytes = Files.size(warcFile);

    List<Long> startOffsets = new ArrayList<>(sampleSize);

    try (InputStream in = new BufferedInputStream(Files.newInputStream(warcFile))) {
      WarcReader reader = compressed ?
          WarcReaderFactory.getReaderCompressed(in) :
          WarcReaderFactory.getReaderUncompressed(in);

      Iterator<WarcRecord> it = reader.iterator();

      for (int i = 0; i < sampleSize && it.hasNext(); i++) {
        startOffsets.add(reader.getStartOffset());
        it.next();
      }
    }

    int sampled = startOffsets.size();

    if (sampled < 2) {
      // Not enough data in the sampled window to estimate a per-record average; report what we
      // observed and decline to extrapolate rather than guess from a single data point.
      return new SampleResult(sizeBytes, compressed, sampled, -1, -1);
    }

    // Average on-disk bytes per record across the sampled window (first record's offset to the
    // start of the record just past the last sampled one, divided by number of gaps).
    double avgBytes = (double) (startOffsets.get(sampled - 1) - startOffsets.get(0)) / (sampled - 1);

    long estimatedTotal = avgBytes > 0 ? Math.round(sizeBytes / avgBytes) : -1;

    return new SampleResult(sizeBytes, compressed, sampled, avgBytes, estimatedTotal);
  }

  /** Computes the SHA-256 checksum of a file, streaming it rather than loading it into memory. */
  public static String sha256(Path file) throws IOException {
    MessageDigest digest;

    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available in this JVM", e);
    }

    byte[] buf = new byte[256 * 1024];

    try (DigestInputStream dis =
             new DigestInputStream(new BufferedInputStream(Files.newInputStream(file)), digest)) {
      while (dis.read(buf) >= 0) {
        // Read to exhaustion; DigestInputStream updates the digest as a side effect.
      }
    }

    StringBuilder sb = new StringBuilder();
    for (byte b : digest.digest()) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  private WarcCorpusStats() {
  }
}
