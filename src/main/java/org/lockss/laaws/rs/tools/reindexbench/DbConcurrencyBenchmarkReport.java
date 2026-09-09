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

import java.util.ArrayList;
import java.util.List;

/**
 * Plain-data mirror of the JSON schema written by lockss-core's {@code
 * org.lockss.rs.io.index.db.ReindexConcurrencyBenchmark.Report} (see {@code
 * lockss-core/src/test/java/org/lockss/rs/io/index/db/ReindexConcurrencyBenchmark.java}). This
 * module ({@code laaws-repository-tools}) has no dependency on lockss-core's test-scoped classes,
 * so this is a deliberate, hand-maintained field-for-field duplicate of that class's Jackson POJOs
 * - NOT the same class, just structurally compatible for deserializing the report it writes. Only
 * ever used to read a DB-concurrency-benchmark report supplied via {@code --db-benchmark-report};
 * this tool never runs that benchmark itself (see {@code ReindexParseBenchmark}'s class javadoc
 * for why it must stay database-free).
 * <p>
 * Only the fields this tool actually consumes are mirrored (each {@code KResult}'s own
 * {@code repetitions} list is intentionally omitted); {@code @JsonIgnoreProperties(ignoreUnknown
 * = true)} everywhere means the source report can carry additional fields without breaking this.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DbConcurrencyBenchmarkReport {
  public String timestamp;
  public String jvm;
  public String os;
  public String postgresVersion;
  public boolean synchronousCommitCorrectedToOn;
  public Params params;
  public List<KResult> results = new ArrayList<>();
  public Long k1DriftRecheckWallClockMs;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Params {
    public int[] kList;
    public int repetitions;
    public int totalArtifacts;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class KResult {
    public int k;
    public double wallClockMsMedian;
    public double wallClockMsMin;
    public double wallClockMsMax;
    public double artifactsPerSecMedian;
    public double speedupVsK1;
    public double parallelEfficiency;
    public double aggregateThreadTimeMsMedian;
  }
}
