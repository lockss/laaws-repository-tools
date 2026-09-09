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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * Plain-data model for a reindex-benchmark corpus manifest: a portable, versioned list of WARC
 * files that make up a benchmark corpus, identified by path and verified by checksum.
 * <p>
 * A manifest never contains WARC bytes. It is portable across machines in the sense that the
 * same manifest file can be handed to different operators, but each operator resolves
 * {@link Entry#relativePath} against their OWN local {@code --corpus-dir}. WARCs produced by
 * independent crawls of nominally "the same" content are essentially NEVER byte-identical in
 * practice - crawl timing, crawl order, and upstream content changes (even something as small as
 * a response Date header) guarantee differences - so two operators' local copies of a corpus
 * described by the same manifest id will not, and are not expected to, hash the same. The
 * checksum here exists ONLY to detect drift or corruption in ONE person's own local copy between
 * two of THEIR OWN runs. It is never a mechanism for confirming that two people benchmarked
 * identical bytes, and this tool must never imply otherwise. Cross-run or cross-person
 * comparability should instead be argued from the corpus statistics recorded in the benchmark
 * report (total bytes, record count, tag/size-class breakdown), not from matching checksums.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorpusManifest {

  public static final int CURRENT_MANIFEST_VERSION = 1;

  private int manifestVersion = CURRENT_MANIFEST_VERSION;
  private String createdAt;
  private String notes;
  private List<Entry> entries = new ArrayList<>();

  public int getManifestVersion() {
    return manifestVersion;
  }

  public void setManifestVersion(int manifestVersion) {
    this.manifestVersion = manifestVersion;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(String createdAt) {
    this.createdAt = createdAt;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
  }

  public List<Entry> getEntries() {
    return entries;
  }

  public void setEntries(List<Entry> entries) {
    this.entries = entries;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Entry {
    private String id;
    private String relativePath;
    private long sizeBytes;
    private String sha256;
    private boolean compressed;
    private Long recordCountHint;
    private List<String> tags = new ArrayList<>();

    public Entry() {
    }

    public Entry(String id, String relativePath, long sizeBytes, String sha256, boolean compressed,
                 Long recordCountHint, List<String> tags) {
      this.id = id;
      this.relativePath = relativePath;
      this.sizeBytes = sizeBytes;
      this.sha256 = sha256;
      this.compressed = compressed;
      this.recordCountHint = recordCountHint;
      this.tags = tags != null ? tags : new ArrayList<>();
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getRelativePath() {
      return relativePath;
    }

    public void setRelativePath(String relativePath) {
      this.relativePath = relativePath;
    }

    public long getSizeBytes() {
      return sizeBytes;
    }

    public void setSizeBytes(long sizeBytes) {
      this.sizeBytes = sizeBytes;
    }

    public String getSha256() {
      return sha256;
    }

    public void setSha256(String sha256) {
      this.sha256 = sha256;
    }

    public boolean isCompressed() {
      return compressed;
    }

    public void setCompressed(boolean compressed) {
      this.compressed = compressed;
    }

    public Long getRecordCountHint() {
      return recordCountHint;
    }

    public void setRecordCountHint(Long recordCountHint) {
      this.recordCountHint = recordCountHint;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> tags) {
      this.tags = tags;
    }

    public boolean hasTag(String tag) {
      return tags != null && tags.contains(tag);
    }

    @Override
    public String toString() {
      return "Entry{id=" + id + ", relativePath=" + relativePath + ", sizeBytes=" + sizeBytes
          + ", compressed=" + compressed + ", tags=" + tags + "}";
    }
  }
}
