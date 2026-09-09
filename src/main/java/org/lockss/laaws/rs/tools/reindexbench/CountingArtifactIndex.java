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

import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.rest.repo.model.VersionsEnum;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.time.Deadline;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A trivial, in-process, no-op {@link ArtifactIndex} used only by {@link ReindexParseBenchmark}.
 * <p>
 * This class exists solely to let the benchmark drive the real, production
 * {@code WarcArtifactDataStore.indexArtifactsFromWarc(...)} WARC-parsing code path without
 * touching a database, a Solr core, or any other real index backend. Every mutating call is a
 * no-op beyond incrementing an in-memory counter; every read-only call returns an empty or null
 * result. This class is NOT a general-purpose {@link ArtifactIndex} implementation and must never
 * be used outside of this benchmarking tool.
 */
public class CountingArtifactIndex implements ArtifactIndex {

  private final AtomicLong artifactsIndexed = new AtomicLong();

  /**
   * Returns the total number of artifacts this index has been asked to (re)index since it was
   * created, across all calling threads.
   */
  public long getArtifactsIndexed() {
    return artifactsIndexed.get();
  }

  public void reset() {
    artifactsIndexed.set(0);
  }

  // *******************************************************************************************
  // * LockssRepositorySubsystem
  // *******************************************************************************************

  @Override
  public void setLockssRepository(BaseLockssRepository repository) {
    // No-op: this stub index has no backing repository.
  }

  @Override
  public void init() {
    // No-op
  }

  @Override
  public void start() {
    // No-op
  }

  @Override
  public void stop() {
    // No-op
  }

  // *******************************************************************************************
  // * Ready
  // *******************************************************************************************

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public void waitReady(Deadline deadline) throws TimeoutException {
    // Always ready; nothing to wait for.
  }

  // *******************************************************************************************
  // * StorageInfoSource
  // *******************************************************************************************

  @Override
  public StorageInfo getStorageInfo() {
    return new StorageInfo("CountingArtifactIndex (no-op)");
  }

  // *******************************************************************************************
  // * ArtifactIndex - version locking (no-op: single logical index, no real concurrency control
  // * needed since nothing is actually stored)
  // *******************************************************************************************

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException {
    // No-op
  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    // No-op
  }

  // *******************************************************************************************
  // * ArtifactIndex - writes: this is the part the benchmark actually exercises
  // *******************************************************************************************

  @Override
  public void indexArtifact(Artifact artifact) throws IOException {
    artifactsIndexed.incrementAndGet();
  }

  @Override
  public void indexArtifacts(Iterable<Artifact> artifacts) throws IOException {
    artifactsIndexed.addAndGet(countOf(artifacts));
  }

  @Override
  public void reindexArtifact(Artifact artifact) throws IOException {
    artifactsIndexed.incrementAndGet();
  }

  /**
   * The method the benchmark's parsing hot loop actually calls, once per {@code BATCH_SIZE}
   * artifacts (and once more for the final partial batch) per WARC file. Counts the batch size
   * and reports zero artifacts skipped/rejected - this stub never rejects anything.
   *
   * @param artifacts The batch of {@link Artifact}s the production parsing code wants indexed.
   * @return {@code 0}: this stub never skips an artifact.
   */
  @Override
  public int reindexArtifacts(Iterable<Artifact> artifacts) throws IOException {
    artifactsIndexed.addAndGet(countOf(artifacts));
    return 0;
  }

  private static long countOf(Iterable<Artifact> artifacts) {
    if (artifacts instanceof java.util.Collection) {
      return ((java.util.Collection<?>) artifacts).size();
    }
    long n = 0;
    for (Artifact ignored : artifacts) {
      n++;
    }
    return n;
  }

  @Override
  public void clearIndex() throws IOException {
    artifactsIndexed.set(0);
  }

  // *******************************************************************************************
  // * ArtifactIndex - reads: none of these are exercised by indexArtifactsFromWarc() in the
  // * production code path this tool links against (the per-record getArtifact() lookup that
  // * once guarded the reindex batch was removed - see E.6 in the reindex work this tool follows
  // * up on), but they must still be implemented to satisfy the interface. All return "nothing
  // * here" answers, which is always safe for a stub index that never stores anything.
  // *******************************************************************************************

  @Override
  public Artifact getArtifact(String artifactUuid) throws IOException {
    return null;
  }

  @Override
  public Artifact getArtifact(UUID artifactUuid) throws IOException {
    return null;
  }

  @Override
  public Artifact commitArtifact(String artifactUuid) throws IOException {
    return null;
  }

  @Override
  public Artifact commitArtifact(UUID artifactUuid) throws IOException {
    return null;
  }

  @Override
  public boolean deleteArtifact(String artifactUuid) throws IOException {
    return false;
  }

  @Override
  public boolean deleteArtifact(UUID artifactUuid) throws IOException {
    return false;
  }

  @Override
  public boolean artifactExists(String artifactUuid) throws IOException {
    return false;
  }

  @Override
  public Artifact updateStorageUrl(String artifactUuid, String storageUrl) throws IOException {
    return null;
  }

  @Override
  public Iterable<String> getNamespaces() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<String> getAuIds(String namespace) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifacts(String namespace, String auid, boolean includeUncommitted)
      throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, boolean includeUncommitted)
      throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String namespace, String auid, String prefix)
      throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace, String auid, String prefix)
      throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String prefix,
                                                                 VersionsEnum versions) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, String url) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace, String url, VersionsEnum versions)
      throws IOException {
    return Collections.emptyList();
  }

  @Override
  public Artifact getArtifact(String namespace, String auid, String url, boolean includeUncommitted)
      throws IOException {
    return null;
  }

  @Override
  public Artifact getArtifactVersion(String namespace, String auid, String url, Integer version,
                                      boolean includeUncommitted) throws IOException {
    return null;
  }

  @Override
  public AuSize auSize(String namespace, String auid) throws IOException {
    return new AuSize();
  }

  @Override
  public void startBulkStore(String namespace, String auid) throws IOException {
    throw new UnsupportedOperationException("Bulk store not supported by CountingArtifactIndex");
  }

  @Override
  public void finishBulkStore(String namespace, String auid, int copyBatchSize) throws IOException {
    throw new UnsupportedOperationException("Bulk store not supported by CountingArtifactIndex");
  }
}
