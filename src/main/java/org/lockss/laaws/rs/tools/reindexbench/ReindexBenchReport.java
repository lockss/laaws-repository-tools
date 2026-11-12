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
 * Plain-data model for a {@link ReindexParseBenchmark} report, serialized/deserialized with
 * Jackson. Fields are public for brevity; this is an internal report format for one tool, not a
 * shared API.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReindexBenchReport {

  /** Facts about the environment and inputs a run used, for after-the-fact interpretation. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RunMetadata {
    public String timestamp;
    public String jvmVersion;
    public String osName;
    public String osVersion;
    public int availableProcessors;

    public String manifestPath;
    /** sha256 of the manifest JSON file itself, so a repeat-report can detect if it changed. */
    public String manifestChecksum;

    public String corpusDir;

    /** "verified" or "skipped" - never omitted; see CorpusManifest's checksum-scope note. */
    public String checksumVerification;

    /** The resolved-and-verified (or resolved-and-unverified, if skipped) entries actually used. */
    public List<EntryUsed> entriesUsed = new ArrayList<>();

    /** Git commit of the lockss-core checkout this run's classes were compiled/resolved from, if
     *  determinable; null with a note in {@link Report#notes} otherwise. */
    public String lockssCoreGitRevision;

    /** Best-effort hardware profile of the machine this run executed on; see {@link HardwareProfile}. */
    public HardwareProfile hardwareProfile = new HardwareProfile();
  }

  /**
   * Best-effort, read-only-probe capture of the physical machine's hardware: CPU model, physical
   * vs. logical core counts, a heterogeneous performance/efficiency core split where determinable
   * (the detail that explains e.g. speedup plateauing well below core count on a P+E-core Apple
   * Silicon Mac), total RAM, and storage/controller info. Every field degrades independently to
   * "unknown"/"not determinable" rather than being omitted or failing the run - see
   * {@link HardwareProfileDetector}, which populates this once per run.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class HardwareProfile {
    /** "macos", "linux", or "unsupported". */
    public String platform;

    /** e.g. "Apple M5", or an Intel/AMD model string. Null/placeholder if not determinable. */
    public String cpuModel = "unknown";

    public Integer physicalCores;
    public Integer logicalCores;

    /** "heterogeneous", "homogeneous or not applicable", etc. - human-readable, see detector. */
    public String coreTopology = "not determinable";

    /** Apple Silicon performance-core count; null unless a heterogeneous split was detected. */
    public Integer performanceCores;
    /** Apple Silicon (or hybrid-Intel, where broken down) efficiency-core count; null unless a
     *  heterogeneous split was detected. */
    public Integer efficiencyCores;

    public Long totalMemBytes;

    /** Best-effort human-readable storage/controller summary; "unknown" if not determinable. */
    public String storageSummary = "unknown";
    /** Which command/mechanism produced {@link #storageSummary}, for provenance. */
    public String storageDetectionMethod;

    /** Free-text note, e.g. explaining an unsupported platform or an unexpected capture failure. */
    public String notes;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EntryUsed {
    public String id;
    public String relativePath;
    public String sha256;
    public long sizeBytes;

    public EntryUsed() {
    }

    public EntryUsed(String id, String relativePath, String sha256, long sizeBytes) {
      this.id = id;
      this.relativePath = relativePath;
      this.sha256 = sha256;
      this.sizeBytes = sizeBytes;
    }
  }

  /** The full parameter set used for this run - also what {@code --repeat-report} reads back. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Parameters {
    public String manifestPath;
    public String corpusDir;
    /** "none" if no --tag filter was applied. */
    public String tag = "none";
    public List<Integer> threads = new ArrayList<>();
    public int repetitions;
    /**
     * Opt-in. Default false: the checksum only ever proves a local corpus file hasn't
     * changed since it was last read into a report - never a cross-machine integrity
     * guarantee (independently-crawled WARCs of nominally the same content are never
     * byte-identical in practice), so most runs don't need to pay for computing it.
     * Pass --verify-checksums to check for drift in your own repeatedly-used local copy.
     */
    public boolean verifyChecksums;

    public Parameters() {
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CorpusSummary {
    public int entryCount;
    public long totalBytes;
    /** Sum of manifest recordCountHint across used entries; null if any entry lacked a hint. */
    public Long totalRecordCountHint;
    public List<String> tagsPresent = new ArrayList<>();
  }

  /** One WARC file's outcome within one repetition of one sweep point. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class WarcOutcome {
    public String entryId;
    public String path;
    public long recordsIndexed;
    public long bytes;
    public long wallClockMs;
    /** Non-null iff this WARC failed; named explicitly rather than silently dropped. */
    public String error;
    /**
     * Non-null iff this WARC parsed "successfully" (no {@link #error}) but something about the
     * outcome deserves a human's attention - e.g. the manifest's {@code recordCountHint} was
     * greater than zero but zero records were actually indexed, which can indicate a silent
     * parse failure (some malformed WARCs are read as an empty iteration rather than throwing).
     * Distinct from {@link #error}: a genuinely empty valid WARC is a real, non-error outcome,
     * so this is a heads-up for manual verification, never a hard failure.
     */
    public String warning;

    public WarcOutcome() {
    }
  }

  /** One full pass over the (possibly tag-filtered) corpus at a given thread count. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RepetitionResult {
    public int repetitionIndex;
    public long wallClockMs;
    public long totalRecordsIndexed;
    public long totalBytesProcessed;
    public double recordsPerSec;
    public double mbPerSec;
    public long aggregateWorkerCpuTimeMs;
    /** Null if the platform CPU-load bean was unavailable; see {@link Report#notes}. */
    public Double avgSystemCpuLoadPercent;
    public long gcTimeMs;
    public List<WarcOutcome> warcs = new ArrayList<>();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class MinMedianMax {
    public double min;
    public double median;
    public double max;

    public MinMedianMax() {
    }

    public MinMedianMax(double min, double median, double max) {
      this.min = min;
      this.median = median;
      this.max = max;
    }
  }

  /** All repetitions at one thread count, plus the summary statistics computed from them. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SweepPointResult {
    public int threadCount;
    public List<RepetitionResult> repetitions = new ArrayList<>();

    public MinMedianMax wallClockMs;
    public MinMedianMax recordsPerSec;
    public MinMedianMax mbPerSec;
    public MinMedianMax aggregateWorkerCpuTimeMs;
    public MinMedianMax avgSystemCpuLoadPercent;
    public MinMedianMax gcTimeMs;

    /** Per-WARC time (ms), pooled across all repetitions at this thread count. */
    public MinMedianMax perWarcTimeMs;

    /** speedup = median(wallClockMs @ N=1) / median(wallClockMs @ this N). */
    public Double speedupVsBaseline;
    /** parallelEfficiency = speedupVsBaseline / threadCount. */
    public Double parallelEfficiency;

    /** Every WARC failure observed across every repetition at this thread count, named explicitly. */
    public List<WarcOutcome> failures = new ArrayList<>();

    /**
     * Every WARC that succeeded (no {@link WarcOutcome#error}) but carries a
     * {@link WarcOutcome#warning} (e.g. a recordCountHint/actual-records mismatch), observed
     * across every repetition at this thread count, named explicitly.
     */
    public List<WarcOutcome> warnings = new ArrayList<>();
  }

  /**
   * One parse-thread-count's contribution to the "projected end-to-end reindex speedup" section -
   * this tool's own measured WARC-parse time at that thread count combined with a DB-commit time
   * extrapolated (by artifact count, not measured on this corpus) from an externally-supplied
   * {@code ReindexConcurrencyBenchmark} report. See {@link DbProjection} for why this
   * extrapolation is treated as legitimate for DB-commit rate but would NOT be for parse rate.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DbProjectionPoint {
    /** The thread count from this tool's own {@code --threads} sweep. */
    public int parseThreadCount;
    /**
     * The thread count actually looked up in the DB benchmark report - the same as
     * {@link #parseThreadCount} when the DB report swept that exact K, otherwise the nearest K
     * the DB report actually measured (never a fabricated interpolated value). Named explicitly
     * here so a reader can see exactly which DB measurement backs each row.
     */
    public int dbBenchmarkKUsed;
    /** {@code artifactsPerSecMedian} read from the DB benchmark report at {@link #dbBenchmarkKUsed}. */
    public double dbArtifactsPerSecMedian;
    /** This tool's own measured median parse wall-clock time (ms) at {@link #parseThreadCount}. */
    public double parseWallClockMsMedian;
    /**
     * {@code (this corpus's total record count) / dbArtifactsPerSecMedian * 1000} - the DB-commit
     * time this run's actual record count would take, extrapolated from the DB benchmark's
     * measured rate.
     */
    public double extrapolatedDbTimeMs;
    /** {@code parseWallClockMsMedian + extrapolatedDbTimeMs}. */
    public double totalTimeMs;
    /**
     * {@code totalTimeMs(parseThreadCount=1) / totalTimeMs(this parseThreadCount)}. Null only if
     * the K=1 row's totalTimeMs could not be computed.
     */
    public Double projectedSpeedup;
  }

  /**
   * The "projected end-to-end reindex speedup" section: this tool's own parse-only measurements
   * combined with a DB-commit rate read from an externally-supplied {@code
   * ReindexConcurrencyBenchmark} report (lockss-core, {@code
   * src/test/java/org/lockss/rs/io/index/db/ReindexConcurrencyBenchmark.java}). Present only when
   * {@code --db-benchmark-report} was supplied; null otherwise (see {@link Report#notes} for the
   * caveat printed instead in that case).
   * <p>
   * <b>Why extrapolating the DB rate across corpora is defensible, unlike doing the same for parse
   * rate:</b> this tool's own parse measurements are never assumed to transfer across corpora of
   * different average artifact size, which is why parse is always measured directly on the real
   * corpus being benchmarked - parse cost is dominated by per-byte decompression, so it scales
   * with corpus bytes and is corpus-shape-dependent. DB-commit cost is different: it is dominated
   * by FIXED per-commit overhead (WAL fsync, batch flush) that is much less sensitive to which
   * specific bytes are being committed, so a DB artifacts/sec rate measured once (on a different,
   * synthetic corpus, via {@code ReindexConcurrencyBenchmark}) is reasonable to extrapolate by
   * ARTIFACT COUNT onto this corpus's actual record count - a combination that would NOT be
   * legitimate the other way around (assuming a parse rate transfers across corpora).
   * <p>
   * <b>What this projection assumes, and does not prove:</b> it models a simple architecture where
   * each of the K worker threads does both parsing AND its own DB commits for the WARCs it is
   * assigned - one thread, start to finish, per WARC, the same K controlling both phases. A more
   * sophisticated producer/consumer split (more parser threads feeding fewer DB-committer threads,
   * since DB commits are cheap after batching) could plausibly do BETTER than this projection.
   * Treat this as a reasonable first-implementation-shaped estimate, not a ceiling on what is
   * achievable.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DbProjection {
    /** Path to the {@code ReindexConcurrencyBenchmark} report supplied via --db-benchmark-report. */
    public String dbBenchmarkReportPath;
    /** That report's own {@code timestamp} field, for provenance - when this DB measurement was taken. */
    public String dbBenchmarkReportTimestamp;
    /**
     * This corpus's total record count (measured, from this run's own K=1 sweep point - never the
     * manifest's recordCountHint, which is only an estimate), used as the numerator for every
     * {@link DbProjectionPoint#extrapolatedDbTimeMs}.
     */
    public long totalRecordCount;
    public List<DbProjectionPoint> points = new ArrayList<>();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Report {
    public RunMetadata runMetadata = new RunMetadata();
    public Parameters parameters = new Parameters();
    public CorpusSummary corpusSummary = new CorpusSummary();
    public List<SweepPointResult> sweepResults = new ArrayList<>();
    public List<String> notes = new ArrayList<>();
    /**
     * Present only when {@code --db-benchmark-report} was supplied for this run; null otherwise.
     * See {@link DbProjection}.
     */
    public DbProjection dbProjection;
  }

  private ReindexBenchReport() {
  }
}
