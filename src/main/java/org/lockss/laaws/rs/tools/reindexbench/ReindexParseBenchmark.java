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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.storage.ReindexResult;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Standalone benchmarking tool that measures how WARC-parse throughput for artifact reindexing
 * scales with thread count, using the REAL production parsing code
 * ({@code WarcArtifactDataStore.indexArtifactsFromWarc(...)}) against a throwaway local data
 * store base path and a no-op, in-memory-counting {@link ArtifactIndex} stub
 * ({@link CountingArtifactIndex}) - never a real database, never Solr, never production data.
 * <p>
 * This tool follows up on production reindex-performance analysis that found WARC parsing (not
 * database commit time) is the larger share of total reindex wall-clock time for most AUs, and
 * asks: does splitting WARC parsing across worker threads (one thread per WARC file, never
 * splitting a single WARC across threads) actually buy usable wall-clock speedup, and where does
 * it stop paying off? It is deliberately NOT production code: no database, no REST calls, no
 * changes to any daemon or repository. It exists only to produce a repeatable, local measurement
 * an operator can inspect before considering any change to the real parallelization of the
 * production reindex path.
 * <p>
 * See the package README for a full operator's guide (manifest authoring, the recommended
 * segmented-then-mixed sweep workflow, and corpus-size guidance).
 */
public class ReindexParseBenchmark {
  private static final L4JLogger log = L4JLogger.getLogger();

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();

  /** How often the background system-CPU-load sampler polls, in milliseconds. */
  private static final long CPU_SAMPLE_INTERVAL_MS = 200;

  // *********************************************************************************************
  // * CLI ENTRY POINT
  // *********************************************************************************************

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(null, "manifest", true, "Path to a corpus manifest JSON file");
    options.addOption(null, "corpus-dir", true,
        "Local directory that manifest entries' relativePath is resolved against. Always "
            + "required, even with --repeat-report: a manifest is portable, a local corpus copy "
            + "is not.");
    options.addOption(null, "tag", true,
        "Only benchmark manifest entries whose tags include this value. Omit to use the whole "
            + "manifest (the final mixed-corpus run).");
    options.addOption(null, "threads", true,
        "Comma-separated list of thread counts to sweep. Default: 1,2,4,8,{cores},{cores*2},"
            + "{cores*4}, computed at runtime and deliberately swept past core count. 1 is always "
            + "included (it is the sweep's baseline row).");
    options.addOption(null, "repetitions", true,
        "Number of times to repeat the full corpus pass at each thread count (default 3). "
            + "Never trust a single-shot number: results are reported as median/min/max.");
    options.addOption(null, "out", true, "Path to write the JSON report to (required)");
    options.addOption(null, "repeat-report", true,
        "Path to a prior report JSON. Reruns with that report's manifest/tag/threads/"
            + "repetitions (NOT its corpus-dir, which must be re-supplied via --corpus-dir), "
            + "writing a new report via --out for direct comparison.");
    options.addOption(null, "scratch-dir", true,
        "Throwaway local-data-store base directory (created if needed). Default: a fresh "
            + "directory under java.io.tmpdir. Never written to except for this tool's own "
            + "bookkeeping directories - no WARC content is copied into it.");
    options.addOption(null, "lockss-core-dir", true,
        "Optional path to a lockss-core git checkout, used only to record its HEAD commit in "
            + "the report for provenance. Best-effort: a few common relative locations are also "
            + "tried automatically; failure to determine this is noted in the report, not fatal.");
    options.addOption(null, "verify-checksums", false,
        "Compute and verify sha256 checksums of corpus files before the run (opt-in). "
            + "Default is OFF: the checksum only ever proves a local file hasn't changed since "
            + "it was last read into a report, never a cross-machine integrity guarantee, and "
            + "computing it over large WARCs is real time most runs don't need to spend. Pass "
            + "this when you specifically want to confirm your own repeatedly-used local corpus "
            + "copy hasn't drifted. The report always records whether verification ran or was "
            + "skipped - a report is a claim about what was measured, and this tool never leaves "
            + "that unstated.");
    options.addOption(null, "db-benchmark-report", true,
        "Optional path to a JSON report written by lockss-core's ReindexConcurrencyBenchmark "
            + "(src/test/java/org/lockss/rs/io/index/db/ReindexConcurrencyBenchmark.java), which "
            + "measures how batched DB-commit throughput scales with concurrent writer threads. "
            + "When supplied, the report gains a 'Projected end-to-end reindex speedup' section "
            + "combining this tool's own measured parse time with a DB-commit time extrapolated "
            + "from that report by artifact count. Omit for the parse-only report (default) - a "
            + "clear caveat is printed in its place explaining why parse-only speedup overstates "
            + "true end-to-end speedup. Fails loudly (naming this path and what went wrong) if "
            + "the file is missing, isn't valid JSON, or doesn't look like a "
            + "ReindexConcurrencyBenchmark report - never silently falls back to the no-DB-data "
            + "case.");
    options.addOption("h", "help", false, "Print this help and a corpus-size sizing note");
    options.addOption(null, "progress-ui", false,
        "Optional, default OFF. Start a tiny local HTTP server (JDK-built-in "
            + "com.sun.net.httpserver.HttpServer only, no web framework dependency) on port "
            + ProgressServer.DEFAULT_PORT + " that serves LIVE sweep progress at /progress and an "
            + "embedded HTML page at / while the sweep runs, for watching a long sweep in "
            + "progress. This is purely an additional convenience: with or without this flag, "
            + "every run always writes the complete JSON report (--out) and a static HTML report "
            + "(same path with .html in place of .json) to disk when it finishes - no server or "
            + "network activity is ever required to get the report.");
    options.addOption(null, "regenerate-report", true,
        "Path to an EXISTING ReindexParseBenchmark JSON report. Recomputes and rewrites only the "
            + "report's DERIVED output (the 'Projected end-to-end reindex speedup' section and/or "
            + "a refreshed HTML render) from that already-measured data - executes NO sweep, reads "
            + "no WARCs, touches no --manifest/--corpus-dir. Different from --repeat-report, which "
            + "RE-RUNS the sweep: this closes the case where the parse sweep already ran (possibly "
            + "expensive) and a ReindexConcurrencyBenchmark report only became available "
            + "afterward. Combine with --db-benchmark-report exactly as in a live sweep; omit it "
            + "to just re-render the JSON/HTML from the existing report's data unchanged (e.g. "
            + "after an HTML-template change). --out is still required. "
            + "--manifest/--corpus-dir/--threads/--repetitions/--repeat-report/--tag/"
            + "--verify-checksums/--scratch-dir/--lockss-core-dir are not applicable in this mode "
            + "and are ignored, with a note printed for each one supplied.");

    CommandLine cmd = new DefaultParser().parse(options, args);

    if (cmd.hasOption("help") || args.length == 0) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("ReindexParseBenchmark", options);
      System.out.println();
      System.out.println("Sizing guidance: start with a few GB of WARC content per sweep point "
          + "(i.e. per --tag size class). Larger corpora take proportionally longer to sweep "
          + "across every thread count x repetition combination without changing what the "
          + "measurement means.");
      return;
    }

    if (cmd.hasOption("regenerate-report")) {
      runRegenerateReport(cmd);
      return;
    }

    if (!cmd.hasOption("out")) {
      throw new ParseException("--out is required");
    }
    Path outPath = Paths.get(cmd.getOptionValue("out"));

    if (!cmd.hasOption("corpus-dir")) {
      throw new ParseException("--corpus-dir is required (even with --repeat-report - "
          + "a manifest is portable, a local corpus copy is not)");
    }
    Path corpusDir = Paths.get(cmd.getOptionValue("corpus-dir"));

    ReindexBenchReport.Parameters params;

    if (cmd.hasOption("repeat-report")) {
      Path priorReportPath = Paths.get(cmd.getOptionValue("repeat-report"));
      ReindexBenchReport.Report priorReport =
          mapper.readValue(priorReportPath.toFile(), ReindexBenchReport.Report.class);

      params = priorReport.parameters;
      log.info("Repeating report {}: manifest={}, tag={}, threads={}, repetitions={}, "
              + "checksumVerification={} (corpus-dir re-supplied as {})",
          priorReportPath, params.manifestPath, params.tag, params.threads, params.repetitions,
          priorReport.runMetadata.checksumVerification, corpusDir);

      // --verify-checksums on THIS invocation overrides the prior report's recorded setting to
      // turn verification ON for the repeat; otherwise the prior setting is inherited as-is
      // (including if the prior run had it on - this flag only ever adds verification, never
      // removes it, since the repeat-report path has no "explicitly force it back off" need:
      // absence of the flag just means "don't change what the prior report did"). See README.
      if (cmd.hasOption("verify-checksums")) {
        params.verifyChecksums = true;
      }
    } else {
      if (!cmd.hasOption("manifest")) {
        throw new ParseException("--manifest is required (or use --repeat-report)");
      }

      params = new ReindexBenchReport.Parameters();
      params.manifestPath = cmd.getOptionValue("manifest");
      params.tag = cmd.hasOption("tag") ? cmd.getOptionValue("tag") : "none";
      params.repetitions = cmd.hasOption("repetitions") ?
          Integer.parseInt(cmd.getOptionValue("repetitions")) : 3;
      params.threads = parseThreadsList(cmd.hasOption("threads") ? cmd.getOptionValue("threads") : null);
      params.verifyChecksums = cmd.hasOption("verify-checksums");
    }

    params.corpusDir = corpusDir.toString();

    Path scratchDir = cmd.hasOption("scratch-dir") ?
        Paths.get(cmd.getOptionValue("scratch-dir")) :
        Files.createTempDirectory("reindexbench-scratch-");

    Path lockssCoreDirHint = cmd.hasOption("lockss-core-dir") ?
        Paths.get(cmd.getOptionValue("lockss-core-dir")) : null;

    // Loaded up front (before the sweep runs), not deferred until after: a bad --db-benchmark-
    // report should fail loudly before spending time on a sweep whose combined-projection section
    // could never be produced anyway. Deliberately NOT stored in ReindexBenchReport.Parameters -
    // that object is exactly what --repeat-report replays wholesale on a later invocation, and a
    // local DB-benchmark-report path is no more portable across machines/time than --corpus-dir
    // is (which --repeat-report already requires re-supplying for the same reason).
    Path dbBenchmarkReportPath = cmd.hasOption("db-benchmark-report") ?
        Paths.get(cmd.getOptionValue("db-benchmark-report")) : null;
    DbConcurrencyBenchmarkReport dbBenchmarkReport = dbBenchmarkReportPath == null ? null :
        loadDbBenchmarkReport(dbBenchmarkReportPath);

    // --progress-ui is purely additive: whether or not it is passed, this tool ALWAYS writes a
    // complete JSON report and a complete static HTML report to disk when the sweep finishes -
    // no server, no network activity, no running process required to read the result. When
    // passed, it ADDITIONALLY starts a tiny local HTTP server (JDK-built-in HttpServer only) so
    // an operator can watch live progress in a browser WHILE a long sweep runs.
    ProgressServer progressServer = null;
    if (cmd.hasOption("progress-ui")) {
      progressServer = new ProgressServer();
      progressServer.start(ProgressServer.DEFAULT_PORT);
    }

    long overallStartMs = System.currentTimeMillis();
    ReindexBenchReport.Report report = run(params, corpusDir, scratchDir, lockssCoreDirHint, progressServer);
    long overallElapsedMs = System.currentTimeMillis() - overallStartMs;

    if (dbBenchmarkReport != null) {
      report.dbProjection = computeDbProjection(report, dbBenchmarkReport, dbBenchmarkReportPath.toString());
      addDbProjectionNotes(report, dbBenchmarkReport, dbBenchmarkReportPath.toString());
    } else {
      addNoDbProjectionCaveat(report);
    }

    writeReports(outPath, report);

    printConsoleSummary(report);

    if (progressServer != null) {
      progressServer.publishFinalReport(report);
      progressServer.publishDone(overallElapsedMs);
      System.out.println("Progress server still running - browse the report, then Ctrl-C to exit.");
      // Deliberately does not stop the server or exit: the whole point of --progress-ui is to
      // let an operator view the final report/chart in a browser after the sweep finishes. The
      // process exits (and the server with it) when the operator is done and interrupts it. The
      // JSON and HTML files above are already written and complete regardless of this.
      Thread.currentThread().join();
    }
  }

  /**
   * Writes the JSON report to {@code outPath} and a static HTML render alongside it (same
   * basename, {@code .html} extension) - the one place both the live-sweep path and
   * {@code --regenerate-report} write output, so they can never diverge in how a report is
   * persisted.
   */
  private static void writeReports(Path outPath, ReindexBenchReport.Report report) throws IOException {
    Path outDir = outPath.toAbsolutePath().getParent() == null ? Paths.get(".") : outPath.toAbsolutePath().getParent();
    Files.createDirectories(outDir);
    prettyWriter.writeValue(outPath.toFile(), report);
    log.info("Wrote report to {}", outPath);
    System.out.println("Wrote report to " + outPath.toAbsolutePath());

    Path htmlOutPath = deriveHtmlPath(outPath);
    String jsonFileName = outPath.getFileName() != null ? outPath.getFileName().toString() : "report.json";
    Files.writeString(htmlOutPath, ReindexBenchReportHtml.render(report, jsonFileName));
    log.info("Wrote HTML report to {}", htmlOutPath);
    System.out.println("Wrote HTML report to " + htmlOutPath.toAbsolutePath()
        + " (open directly in a browser - no server needed)");
  }

  /**
   * Implements {@code --regenerate-report}: recomputes and rewrites ONLY a report's derived
   * output (the "Projected end-to-end reindex speedup" section, and the HTML render) from an
   * already-produced {@code ReindexParseBenchmark} JSON report - no sweep is executed, no WARC is
   * read, {@code --manifest}/{@code --corpus-dir} are never touched. Reuses exactly the same
   * {@link #computeDbProjection}/{@link #addDbProjectionNotes}/{@link #addNoDbProjectionCaveat}
   * functions the live-sweep path in {@link #main} uses, so the combining logic (parse time +
   * extrapolated DB time -&gt; projected speedup) is never duplicated between the two paths.
   */
  private static void runRegenerateReport(CommandLine cmd) throws IOException, ParseException {
    if (!cmd.hasOption("out")) {
      throw new ParseException("--out is required (even in --regenerate-report mode)");
    }
    Path outPath = Paths.get(cmd.getOptionValue("out"));

    String[] inapplicableOptions = {"manifest", "corpus-dir", "threads", "repetitions",
        "repeat-report", "tag", "verify-checksums", "scratch-dir", "lockss-core-dir"};
    for (String opt : inapplicableOptions) {
      if (cmd.hasOption(opt)) {
        String msg = "--regenerate-report executes no sweep and reads no WARCs, so --" + opt
            + " is not applicable in this mode and was IGNORED.";
        log.warn(msg);
        System.out.println("Note: " + msg);
      }
    }

    Path regenPath = Paths.get(cmd.getOptionValue("regenerate-report"));
    if (!Files.isRegularFile(regenPath)) {
      throw new IOException("--regenerate-report path does not exist or is not a regular file: "
          + regenPath.toAbsolutePath());
    }
    if (outPath.toAbsolutePath().normalize().equals(regenPath.toAbsolutePath().normalize())) {
      throw new IOException("--out (" + outPath.toAbsolutePath() + ") must not be the same file "
          + "as --regenerate-report - this tool never overwrites its own input report, even in "
          + "this mode; pass a different --out path");
    }

    ReindexBenchReport.Report report;
    try {
      report = mapper.readValue(regenPath.toFile(), ReindexBenchReport.Report.class);
    } catch (IOException e) {
      throw new IOException("--regenerate-report at " + regenPath.toAbsolutePath()
          + " could not be parsed as a ReindexParseBenchmark report: " + e.getMessage(), e);
    }
    if (report.sweepResults == null || report.sweepResults.isEmpty()) {
      throw new IOException("--regenerate-report at " + regenPath.toAbsolutePath()
          + " does not look like a ReindexParseBenchmark report: no 'sweepResults' entries found");
    }

    // Drop any dbProjection/db-related notes this report may already carry from a prior run,
    // before recomputing - makes --regenerate-report idempotent under repeated invocations
    // rather than accumulating duplicate notes across runs.
    report.dbProjection = null;
    report.notes.removeIf(n -> n != null && (
        n.startsWith("Projected end-to-end reindex speedup assumes")
            || n.startsWith("The DB-commit rate used above")
            || n.startsWith("This report measures WARC-parse speedup ONLY")));

    Path dbBenchmarkReportPath = cmd.hasOption("db-benchmark-report") ?
        Paths.get(cmd.getOptionValue("db-benchmark-report")) : null;
    if (dbBenchmarkReportPath != null) {
      DbConcurrencyBenchmarkReport dbReport = loadDbBenchmarkReport(dbBenchmarkReportPath);
      report.dbProjection = computeDbProjection(report, dbReport, dbBenchmarkReportPath.toString());
      addDbProjectionNotes(report, dbReport, dbBenchmarkReportPath.toString());
    } else {
      addNoDbProjectionCaveat(report);
    }

    writeReports(outPath, report);
    printConsoleSummary(report);
  }

  /**
   * Same basename as {@code outPath} with its extension replaced by {@code .html}. If that would
   * collide with {@code outPath} itself (e.g. an unusual {@code --out foo.html}), an extra
   * {@code .html} suffix is appended instead of silently overwriting the just-written JSON file.
   */
  private static Path deriveHtmlPath(Path outPath) {
    String fileName = outPath.getFileName() != null ? outPath.getFileName().toString() : "report.json";
    int dot = fileName.lastIndexOf('.');
    String htmlFileName = (dot >= 0 ? fileName.substring(0, dot) : fileName) + ".html";
    Path parent = outPath.getParent();
    Path candidate = parent == null ? Paths.get(htmlFileName) : parent.resolve(htmlFileName);
    if (candidate.equals(outPath)) {
      candidate = parent == null ? Paths.get(fileName + ".html") : parent.resolve(fileName + ".html");
    }
    return candidate;
  }

  /**
   * Prints a short human-readable summary of anything a person should look at without opening
   * the JSON report: WARC failures (including magic-byte rejections and widened Throwable
   * catches) and recordCountHint-mismatch warnings, both named explicitly by manifest id/path.
   */
  private static void printConsoleSummary(ReindexBenchReport.Report report) {
    List<ReindexBenchReport.WarcOutcome> allFailures = report.sweepResults.stream()
        .flatMap(sp -> sp.failures.stream())
        .collect(Collectors.toList());
    List<ReindexBenchReport.WarcOutcome> allWarnings = report.sweepResults.stream()
        .flatMap(sp -> sp.warnings.stream())
        .collect(Collectors.toList());

    if (allFailures.isEmpty() && allWarnings.isEmpty()) {
      System.out.println("No WARC failures or warnings across the sweep.");
      return;
    }

    if (!allFailures.isEmpty()) {
      System.out.println();
      System.out.println("WARC FAILURES (" + allFailures.size() + " occurrence(s) across the sweep):");
      for (ReindexBenchReport.WarcOutcome w : allFailures) {
        System.out.println("  [error] entryId=" + w.entryId + " path=" + w.path + ": " + w.error);
      }
    }

    if (!allWarnings.isEmpty()) {
      System.out.println();
      System.out.println("WARC WARNINGS (" + allWarnings.size() + " occurrence(s) across the sweep):");
      for (ReindexBenchReport.WarcOutcome w : allWarnings) {
        System.out.println("  [warning] entryId=" + w.entryId + " path=" + w.path + ": " + w.warning);
      }
    }
  }

  private static List<Integer> parseThreadsList(String csv) {
    if (csv == null || csv.trim().isEmpty()) {
      int cores = Runtime.getRuntime().availableProcessors();
      Set<Integer> defaults = new TreeSet<>(Arrays.asList(1, 2, 4, 8, cores, cores * 2, cores * 4));
      return new ArrayList<>(defaults);
    }

    Set<Integer> parsed = new TreeSet<>();
    for (String s : csv.split(",")) {
      parsed.add(Integer.parseInt(s.trim()));
    }
    parsed.add(1); // The baseline row is mandatory; see class javadoc / README.
    return new ArrayList<>(parsed);
  }

  // *********************************************************************************************
  // * RUN
  // *********************************************************************************************

  static ReindexBenchReport.Report run(ReindexBenchReport.Parameters params, Path corpusDir,
                                        Path scratchDir, Path lockssCoreDirHint) throws IOException {
    return run(params, corpusDir, scratchDir, lockssCoreDirHint, null);
  }

  static ReindexBenchReport.Report run(ReindexBenchReport.Parameters params, Path corpusDir,
                                        Path scratchDir, Path lockssCoreDirHint,
                                        ProgressServer progressServer) throws IOException {
    ReindexBenchReport.Report report = new ReindexBenchReport.Report();
    report.parameters = params;

    Path manifestPath = Paths.get(params.manifestPath);
    CorpusManifest manifest = mapper.readValue(manifestPath.toFile(), CorpusManifest.class);

    List<CorpusManifest.Entry> selected = manifest.getEntries().stream()
        .filter(e -> "none".equals(params.tag) || params.tag == null || e.hasTag(params.tag))
        .collect(Collectors.toList());

    if (selected.isEmpty()) {
      throw new IllegalArgumentException("No manifest entries matched tag '" + params.tag + "'");
    }

    // Resolve and verify (unless skipped) every selected entry against the local corpus-dir.
    // Failure here is loud and fatal by design: a benchmark over silently-wrong or missing input
    // is worse than no benchmark.
    List<ResolvedEntry> resolved = new ArrayList<>();
    long totalBytes = 0;
    Long totalRecordHint = 0L;
    boolean anyMissingHint = false;

    for (CorpusManifest.Entry e : selected) {
      Path p = corpusDir.resolve(e.getRelativePath());

      if (!Files.isRegularFile(p)) {
        throw new IOException("Manifest entry '" + e.getId() + "' resolves to a missing file: " + p);
      }

      long actualSize = Files.size(p);
      if (actualSize != e.getSizeBytes()) {
        throw new IOException("Manifest entry '" + e.getId() + "' size mismatch at " + p
            + ": manifest says " + e.getSizeBytes() + " bytes, file is " + actualSize + " bytes");
      }

      String sha256ForReport;
      if (params.verifyChecksums) {
        String actual = WarcCorpusStats.sha256(p);
        if (!actual.equalsIgnoreCase(e.getSha256())) {
          throw new IOException("Manifest entry '" + e.getId() + "' checksum mismatch at " + p
              + ": manifest sha256=" + e.getSha256() + ", actual sha256=" + actual);
        }
        sha256ForReport = actual;
      } else {
        sha256ForReport = e.getSha256();
      }

      resolved.add(new ResolvedEntry(e, p));
      totalBytes += actualSize;
      if (e.getRecordCountHint() != null) {
        totalRecordHint += e.getRecordCountHint();
      } else {
        anyMissingHint = true;
      }

      report.runMetadata.entriesUsed.add(
          new ReindexBenchReport.EntryUsed(e.getId(), e.getRelativePath(), sha256ForReport, actualSize));
    }

    report.runMetadata.checksumVerification = params.verifyChecksums ? "verified" : "skipped";
    report.corpusSummary.entryCount = resolved.size();
    report.corpusSummary.totalBytes = totalBytes;
    report.corpusSummary.totalRecordCountHint = anyMissingHint ? null : totalRecordHint;
    report.corpusSummary.tagsPresent = manifest.getEntries().stream()
        .flatMap(e -> e.getTags().stream())
        .distinct()
        .sorted()
        .collect(Collectors.toList());

    fillRunMetadata(report, manifestPath, params, lockssCoreDirHint);

    report.notes.add("Results depend on this machine's local corpus copy. WARCs from "
        + "independent crawls of nominally the same content are essentially never "
        + "byte-identical in practice (crawl timing, crawl order, and upstream content changes "
        + "all guarantee differences), so this run is NOT guaranteed comparable to another "
        + "person's run of the same manifest id purely because the manifest id matches. Compare "
        + "runs using the corpus statistics recorded above (entryCount, totalBytes, "
        + "totalRecordCountHint, tagsPresent) and in runMetadata.entriesUsed, not by expecting "
        + "matching checksums across machines.");

    if ("skipped".equals(report.runMetadata.checksumVerification)) {
      report.notes.add("Checksum verification was skipped, which is the default: the sha256 "
          + "values under runMetadata.entriesUsed are copied from the manifest, not recomputed. "
          + "This is expected for most runs - the checksum only ever proves a local file hasn't "
          + "changed since it was last read into a report. Pass --verify-checksums if you "
          + "specifically want to confirm your own repeatedly-used local corpus copy hasn't "
          + "drifted since it was checksummed.");
    }

    // Build the local data store once and reuse it for the whole sweep: indexArtifactsFromWarc()
    // touches only its arguments and stateless path-derivation helpers (verified by reading
    // WarcArtifactDataStore's source before writing this tool - see final report), never the
    // store's repo/tmpWarcPool fields, so sharing one instance across worker threads is safe.
    Files.createDirectories(scratchDir);
    LocalWarcArtifactDataStore store = new LocalWarcArtifactDataStore(scratchDir);

    long overallStartMs = System.currentTimeMillis();

    for (int threadCount : params.threads) {
      List<ReindexBenchReport.RepetitionResult> reps = new ArrayList<>();

      for (int r = 0; r < params.repetitions; r++) {
        log.info("Sweep point threads={} repetition={}/{}", threadCount, r + 1, params.repetitions);
        reps.add(runOnePass(store, resolved, threadCount, r, progressServer, overallStartMs,
            params.threads, r + 1, params.repetitions));
      }

      report.sweepResults.add(summarizeSweepPoint(threadCount, reps));
    }

    computeSpeedups(report.sweepResults);

    return report;
  }

  private static void fillRunMetadata(ReindexBenchReport.Report report, Path manifestPath,
                                       ReindexBenchReport.Parameters params, Path lockssCoreDirHint)
      throws IOException {
    ReindexBenchReport.RunMetadata rm = report.runMetadata;
    rm.timestamp = Instant.now().toString();
    rm.jvmVersion = System.getProperty("java.version");
    rm.osName = System.getProperty("os.name");
    rm.osVersion = System.getProperty("os.version");
    rm.availableProcessors = Runtime.getRuntime().availableProcessors();
    rm.manifestPath = manifestPath.toAbsolutePath().toString();
    rm.manifestChecksum = WarcCorpusStats.sha256(manifestPath);
    rm.corpusDir = params.corpusDir;

    rm.hardwareProfile = HardwareProfileDetector.capture();

    rm.lockssCoreGitRevision = resolveLockssCoreGitRevision(lockssCoreDirHint);
    if (rm.lockssCoreGitRevision == null) {
      report.notes.add("Could not determine lockss-core's git HEAD revision (no working "
          + "--lockss-core-dir given and no common relative checkout location was found). The "
          + "class files this run actually linked against came from whatever lockss-core-bundle "
          + "version was resolved on the Maven classpath - see the build's effective pom for that "
          + "version if exact provenance matters.");
    }
  }

  private static String resolveLockssCoreGitRevision(Path hint) {
    List<Path> candidates = new ArrayList<>();
    if (hint != null) {
      candidates.add(hint);
    }
    // Best-effort common relative locations, tried only if no hint was given or the hint failed.
    candidates.add(Paths.get("../lockss-core"));
    candidates.add(Paths.get("../lockss-core.develop"));
    candidates.add(Paths.get("lockss-core"));
    candidates.add(Paths.get("lockss-core.develop"));

    for (Path candidate : candidates) {
      if (!Files.isDirectory(candidate)) {
        continue;
      }
      try {
        ProcessBuilder pb = new ProcessBuilder("git", "-C", candidate.toString(), "rev-parse", "HEAD");
        pb.redirectErrorStream(false);
        Process proc = pb.start();
        String output;
        try (var in = proc.getInputStream()) {
          output = new String(in.readAllBytes()).trim();
        }
        boolean finished = proc.waitFor(5, TimeUnit.SECONDS);
        if (finished && proc.exitValue() == 0 && !output.isEmpty()) {
          return output + " (from " + candidate.toAbsolutePath() + ")";
        }
      } catch (Exception e) {
        log.debug("git rev-parse against {} failed, trying next candidate", candidate, e);
      }
    }
    return null;
  }

  // *********************************************************************************************
  // * PROJECTED END-TO-END REINDEX SPEEDUP (combining this tool's parse measurements with an
  // * externally-supplied ReindexConcurrencyBenchmark DB-commit-rate report)
  // *********************************************************************************************

  /**
   * Reads and validates a {@code ReindexConcurrencyBenchmark} JSON report from {@code path}.
   * Fails loudly (naming the path and what went wrong) rather than returning null or silently
   * falling back to the no-DB-data case - a {@code --db-benchmark-report} that was explicitly
   * requested but couldn't be used must never be treated as though it was never passed.
   */
  private static DbConcurrencyBenchmarkReport loadDbBenchmarkReport(Path path) throws IOException {
    if (!Files.isRegularFile(path)) {
      throw new IOException("--db-benchmark-report path does not exist or is not a regular file: "
          + path.toAbsolutePath());
    }

    DbConcurrencyBenchmarkReport dbReport;
    try {
      dbReport = mapper.readValue(path.toFile(), DbConcurrencyBenchmarkReport.class);
    } catch (IOException e) {
      throw new IOException("--db-benchmark-report at " + path.toAbsolutePath()
          + " could not be parsed as JSON: " + e.getMessage(), e);
    }

    if (dbReport.results == null || dbReport.results.isEmpty()) {
      throw new IOException("--db-benchmark-report at " + path.toAbsolutePath()
          + " does not look like a ReindexConcurrencyBenchmark report: no 'results' entries found "
          + "(expected a JSON object with a non-empty 'results' array, each entry carrying 'k' and "
          + "'artifactsPerSecMedian' fields)");
    }
    for (DbConcurrencyBenchmarkReport.KResult kr : dbReport.results) {
      if (!(kr.artifactsPerSecMedian > 0) || Double.isInfinite(kr.artifactsPerSecMedian)) {
        // ReindexConcurrencyBenchmark itself writes +Infinity when its measured wallClockMsMedian
        // is <= 0 (a degenerate, effectively-instant run) - not usable as an extrapolation rate.
        throw new IOException("--db-benchmark-report at " + path.toAbsolutePath()
            + " has a non-positive or non-finite artifactsPerSecMedian (" + kr.artifactsPerSecMedian
            + ") for k=" + kr.k + " - cannot extrapolate DB-commit time from this report");
      }
    }
    return dbReport;
  }

  /**
   * Picks the DB benchmark's own measured K value nearest to {@code targetK} (never a fabricated
   * interpolated value between two measured K's). Ties are broken toward the smaller K,
   * deterministically.
   */
  private static DbConcurrencyBenchmarkReport.KResult pickNearestK(
      List<DbConcurrencyBenchmarkReport.KResult> results, int targetK) {
    DbConcurrencyBenchmarkReport.KResult best = null;
    int bestDiff = Integer.MAX_VALUE;
    for (DbConcurrencyBenchmarkReport.KResult r : results) {
      int diff = Math.abs(r.k - targetK);
      if (best == null || diff < bestDiff || (diff == bestDiff && r.k < best.k)) {
        best = r;
        bestDiff = diff;
      }
    }
    return best;
  }

  /**
   * This corpus's total record count, measured (never the manifest's recordCountHint, which is
   * only an estimate) from the K=1 (baseline) sweep point's own repetitions - the same corpus is
   * processed at every thread count, so any sweep point's measured record count would do, but the
   * baseline is used for consistency with how {@link #computeSpeedups} anchors everything else.
   */
  private static long measuredTotalRecordCount(ReindexBenchReport.SweepPointResult baseline) {
    List<Long> values = baseline.repetitions.stream()
        .map(r -> r.totalRecordsIndexed)
        .sorted()
        .collect(Collectors.toList());
    int n = values.size();
    if (n == 0) {
      throw new IllegalStateException("Cannot compute DB projection: baseline (threadCount=1) "
          + "sweep point has no repetitions");
    }
    return (n % 2 == 1) ? values.get(n / 2) : (values.get(n / 2 - 1) + values.get(n / 2)) / 2;
  }

  /**
   * Computes the "Projected end-to-end reindex speedup" section: for every parse thread count K in
   * this run's own {@code --threads} sweep, looks up the DB benchmark's measured
   * artifactsPerSecMedian at the nearest K it actually measured, extrapolates a DB-commit time for
   * this corpus's actual record count, and combines it with this run's own measured parse time.
   * See {@link ReindexBenchReport.DbProjection}'s javadoc for the full reasoning and assumptions.
   */
  private static ReindexBenchReport.DbProjection computeDbProjection(
      ReindexBenchReport.Report report, DbConcurrencyBenchmarkReport dbReport, String dbReportPath) {

    ReindexBenchReport.SweepPointResult baseline = report.sweepResults.stream()
        .filter(sp -> sp.threadCount == 1)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Cannot compute DB projection: no baseline "
            + "(threadCount=1) sweep point in this run's own --threads sweep"));

    ReindexBenchReport.DbProjection proj = new ReindexBenchReport.DbProjection();
    proj.dbBenchmarkReportPath = dbReportPath;
    proj.dbBenchmarkReportTimestamp = dbReport.timestamp;
    proj.totalRecordCount = measuredTotalRecordCount(baseline);

    for (ReindexBenchReport.SweepPointResult sp : report.sweepResults) {
      DbConcurrencyBenchmarkReport.KResult nearest = pickNearestK(dbReport.results, sp.threadCount);

      ReindexBenchReport.DbProjectionPoint pt = new ReindexBenchReport.DbProjectionPoint();
      pt.parseThreadCount = sp.threadCount;
      pt.dbBenchmarkKUsed = nearest.k;
      pt.dbArtifactsPerSecMedian = nearest.artifactsPerSecMedian;
      pt.parseWallClockMsMedian = sp.wallClockMs.median;
      pt.extrapolatedDbTimeMs = (proj.totalRecordCount / nearest.artifactsPerSecMedian) * 1000.0;
      pt.totalTimeMs = pt.parseWallClockMsMedian + pt.extrapolatedDbTimeMs;
      proj.points.add(pt);
    }

    Double totalTimeMsAtK1 = proj.points.stream()
        .filter(pt -> pt.parseThreadCount == 1)
        .map(pt -> pt.totalTimeMs)
        .findFirst()
        .orElse(null);
    for (ReindexBenchReport.DbProjectionPoint pt : proj.points) {
      pt.projectedSpeedup = (totalTimeMsAtK1 == null || pt.totalTimeMs <= 0) ?
          null : totalTimeMsAtK1 / pt.totalTimeMs;
    }

    return proj;
  }

  private static void addDbProjectionNotes(ReindexBenchReport.Report report,
      DbConcurrencyBenchmarkReport dbReport, String dbReportPath) {
    report.notes.add("Projected end-to-end reindex speedup assumes a simple architecture where "
        + "each of the K worker threads does both parsing AND its own DB commits for the WARCs it "
        + "is assigned - one thread, start to finish, per WARC, the same K controlling both "
        + "phases. A more sophisticated producer/consumer split (more parser threads feeding fewer "
        + "DB-committer threads, since DB commits are cheap after batching) could plausibly do "
        + "BETTER than this projection - treat it as a reasonable first-implementation-shaped "
        + "estimate, not a ceiling on what is achievable.");
    report.notes.add("The DB-commit rate used above (artifactsPerSecMedian per thread count) was "
        + "measured on a DIFFERENT, synthetic corpus by ReindexConcurrencyBenchmark (report at "
        + dbReportPath + ", timestamp " + (dbReport.timestamp == null ? "unknown" : dbReport.timestamp)
        + "), not measured on this corpus directly. It is extrapolated onto this corpus's actual "
        + "record count by ARTIFACT COUNT, which is legitimate here (unlike a similar cross-corpus "
        + "extrapolation would be for parse time) because DB-commit cost is dominated by fixed "
        + "per-commit overhead (WAL fsync, batch flush) that is largely insensitive to which "
        + "specific bytes are committed - whereas WARC-parse cost is dominated by per-byte "
        + "decompression and scales with corpus bytes, which is exactly why this tool always "
        + "measures parse time directly on the real corpus rather than assuming it transfers.");
  }

  private static void addNoDbProjectionCaveat(ReindexBenchReport.Report report) {
    report.notes.add("This report measures WARC-parse speedup ONLY - database-commit time is a "
        + "real fraction of total reindex time that is NOT accounted for in the numbers above. "
        + "Earlier measurement in this project (the WARC-vs-DB split investigation) found DB-"
        + "commit time to be roughly 20-35% of total reindex time for typical real content in the "
        + "30KB-400KB average-artifact-size range, and that this DB share SHRINKS further as "
        + "average artifact size grows (a large-average-artifact AU is dominated by decompression, "
        + "nearly regardless of DB behavior) - see the 'WARC-read/parse time vs. DB-commit time' "
        + "addendum in au-reindex-recovery-plan.md for the full measurement. Consequently, actual "
        + "end-to-end reindex speedup will be somewhat LOWER than the parse-only speedup numbers "
        + "shown above, not equal to them. Pass --db-benchmark-report <path> (from a "
        + "ReindexConcurrencyBenchmark run) to get a combined 'Projected end-to-end reindex "
        + "speedup' section instead of this caveat.");
  }

  // *********************************************************************************************
  // * ONE SWEEP-POINT PASS
  // *********************************************************************************************

  private static class ResolvedEntry {
    final CorpusManifest.Entry entry;
    final Path path;

    ResolvedEntry(CorpusManifest.Entry entry, Path path) {
      this.entry = entry;
      this.path = path;
    }
  }

  private static ReindexBenchReport.RepetitionResult runOnePass(
      LocalWarcArtifactDataStore store, List<ResolvedEntry> entries, int threadCount, int repetitionIndex)
      throws IOException {
    return runOnePass(store, entries, threadCount, repetitionIndex, null, 0,
        Collections.singletonList(threadCount), repetitionIndex + 1, 1);
  }

  private static ReindexBenchReport.RepetitionResult runOnePass(
      LocalWarcArtifactDataStore store, List<ResolvedEntry> entries, int threadCount, int repetitionIndex,
      ProgressServer progressServer, long overallStartMs, List<Integer> allThreadCounts,
      int repetitionOneBased, int totalRepetitions)
      throws IOException {

    CountingArtifactIndex index = new CountingArtifactIndex();
    AtomicLong warcsCompleted = new AtomicLong();

    if (progressServer != null) {
      progressServer.publish(makeProgress(threadCount, allThreadCounts, repetitionOneBased,
          totalRepetitions, 0, entries.size(), overallStartMs));
    }

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    boolean cpuTimeSupported = threadMXBean.isThreadCpuTimeSupported();
    if (cpuTimeSupported && !threadMXBean.isThreadCpuTimeEnabled()) {
      threadMXBean.setThreadCpuTimeEnabled(true);
    }

    long gcTimeBefore = sumGcCollectionTime();

    AtomicLong aggregateWorkerCpuTimeNs = new AtomicLong();
    CpuLoadSampler sampler = new CpuLoadSampler();
    sampler.start();

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<ReindexBenchReport.WarcOutcome>> futures = new ArrayList<>();

    long wallStartNs = System.nanoTime();

    for (ResolvedEntry re : entries) {
      Callable<ReindexBenchReport.WarcOutcome> task = () -> {
        ReindexBenchReport.WarcOutcome outcome = new ReindexBenchReport.WarcOutcome();
        outcome.entryId = re.entry.getId();
        outcome.path = re.path.toString();
        outcome.bytes = re.entry.getSizeBytes();

        long taskStartNs = System.nanoTime();
        long cpuStartNs = cpuTimeSupported ? threadMXBean.getCurrentThreadCpuTime() : -1;

        try {
          // Cheap sanity check before the expensive parse: read only the first few bytes and
          // confirm they look like the file type the manifest claims (gzip magic for a
          // compressed WARC, "WARC/" for an uncompressed one). The underlying JWAT reader
          // treats "no valid WARC records found by EOF" as an empty, successful iteration
          // rather than throwing, so without this check a file of pure garbage bytes named
          // *.warc is silently reported as recordsIndexed=0 with no error - indistinguishable
          // from a genuinely empty valid WARC. Catch this loudly, before ever calling into
          // indexArtifactsFromWarc for this file.
          String magicError = checkWarcMagicBytes(re.path, re.entry.isCompressed());
          if (magicError != null) {
            outcome.error = magicError;
          } else {
            long numIndexed = store.indexArtifactsFromWarc(index, re.path, new ReindexResult());
            outcome.recordsIndexed = numIndexed;

            Long hint = re.entry.getRecordCountHint();
            if (hint != null && hint > 0 && numIndexed == 0) {
              outcome.warning = "Manifest recordCountHint was " + hint + " but 0 records were "
                  + "actually indexed - possible silent parse failure, verify this file manually.";
              log.warn("Reindex-parse benchmark: recordCountHint/actual-records mismatch "
                      + "[entryId: {}, path: {}, hint: {}, actual: 0]",
                  re.entry.getId(), re.path, hint);
            }
          }
        } catch (Throwable t) {
          // Widened from Exception to Throwable: a genuinely corrupt/malicious WARC (e.g. a
          // garbage length field triggering a huge allocation) could plausibly throw an Error
          // (OutOfMemoryError, StackOverflowError, etc.), which a narrower catch would let
          // propagate up through this Future and abort the entire sweep pass via Future.get()
          // below, rather than being recorded as just this one file's failure. Every per-file
          // failure mode must end up as that file's outcome.error, never as an aborted pass.
          log.error("Reindex-parse benchmark: WARC failed [entryId: {}, path: {}]",
              re.entry.getId(), re.path, t);
          outcome.error = (t instanceof Error ? "[" + t.getClass().getSimpleName() + "] " : "")
              + t.toString();
        } finally {
          long taskEndNs = System.nanoTime();
          outcome.wallClockMs = (taskEndNs - taskStartNs) / 1_000_000;

          if (cpuTimeSupported) {
            long cpuEndNs = threadMXBean.getCurrentThreadCpuTime();
            if (cpuStartNs >= 0 && cpuEndNs >= 0) {
              aggregateWorkerCpuTimeNs.addAndGet(cpuEndNs - cpuStartNs);
            }
          }

          if (progressServer != null) {
            long completed = warcsCompleted.incrementAndGet();
            progressServer.publish(makeProgress(threadCount, allThreadCounts, repetitionOneBased,
                totalRepetitions, completed, entries.size(), overallStartMs));
          }
        }

        return outcome;
      };

      futures.add(executor.submit(task));
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
        throw new IllegalStateException("Reindex-parse benchmark pass did not finish within 1 day");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for reindex-parse benchmark pass", e);
    }

    long wallEndNs = System.nanoTime();
    sampler.stopAndJoin();
    long gcTimeAfter = sumGcCollectionTime();

    ReindexBenchReport.RepetitionResult result = new ReindexBenchReport.RepetitionResult();
    result.repetitionIndex = repetitionIndex;
    result.wallClockMs = (wallEndNs - wallStartNs) / 1_000_000;
    result.gcTimeMs = gcTimeAfter - gcTimeBefore;
    result.aggregateWorkerCpuTimeMs = aggregateWorkerCpuTimeNs.get() / 1_000_000;
    result.avgSystemCpuLoadPercent = sampler.averageLoadPercent(); // null if unavailable

    long totalRecords = 0;
    long totalBytes = 0;

    for (Future<ReindexBenchReport.WarcOutcome> f : futures) {
      ReindexBenchReport.WarcOutcome outcome;
      try {
        outcome = f.get();
      } catch (Exception e) {
        throw new RuntimeException("Reindex-parse benchmark task failed unexpectedly", e);
      }
      result.warcs.add(outcome);
      if (outcome.error == null) {
        totalRecords += outcome.recordsIndexed;
        totalBytes += outcome.bytes;
      }
    }

    result.totalRecordsIndexed = totalRecords;
    result.totalBytesProcessed = totalBytes;

    double seconds = result.wallClockMs / 1000.0;
    result.recordsPerSec = seconds > 0 ? totalRecords / seconds : 0;
    result.mbPerSec = seconds > 0 ? (totalBytes / (1024.0 * 1024.0)) / seconds : 0;

    long countedByIndex = index.getArtifactsIndexed();
    if (countedByIndex != totalRecords) {
      // Defensive cross-check: the stub index's own counter should agree with the sum of
      // per-WARC return values. A mismatch means either return-value accounting above or the
      // stub's counting is wrong; surface it loudly rather than silently trusting one source.
      log.warn("Reindex-parse benchmark: stub index counted {} artifacts but per-WARC return "
          + "values summed to {} at threadCount={}, repetition={}", countedByIndex, totalRecords,
          threadCount, repetitionIndex);
    }

    return result;
  }

  /** Gzip magic bytes: the first two bytes of every valid gzip stream. */
  private static final byte[] GZIP_MAGIC = new byte[]{(byte) 0x1F, (byte) 0x8B};
  /** ASCII prefix every valid (uncompressed) WARC record/file begins with (WARC/1.0, WARC/1.1, ...). */
  private static final byte[] WARC_MAGIC = "WARC/".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  /**
   * Cheap sanity check, reading only the first few bytes of {@code path}: does it begin with the
   * magic bytes expected for its declared compression state? Returns a clear, specific error
   * message if not (so the caller can set {@code outcome.error} and skip the file entirely
   * without ever calling into the expensive real parser), or {@code null} if the check passes.
   * This is NOT a full WARC validity check - it is a fast filter for "this obviously isn't even
   * the right kind of file," which JWAT does not itself perform (a non-WARC file can iterate as
   * empty rather than throwing; see class/README notes).
   */
  private static String checkWarcMagicBytes(Path path, boolean compressed) throws IOException {
    byte[] expected = compressed ? GZIP_MAGIC : WARC_MAGIC;
    byte[] actual = new byte[expected.length];
    int read;
    try (InputStream in = Files.newInputStream(path)) {
      read = in.readNBytes(actual, 0, actual.length);
    }
    if (read < expected.length || !Arrays.equals(expected, Arrays.copyOf(actual, expected.length))) {
      String kind = compressed ? "gzip magic bytes (0x1F 0x8B)" : "ASCII \"WARC/\"";
      return "File does not begin with valid WARC/gzip magic bytes - not a readable WARC file "
          + "(expected " + kind + " for a " + (compressed ? "compressed (.warc.gz)" : "uncompressed (.warc)")
          + " entry, per the manifest's 'compressed' flag, but the file's actual leading bytes "
          + "did not match)";
    }
    return null;
  }

  private static ProgressServer.Progress makeProgress(int threadCount, List<Integer> allThreadCounts,
      int repetitionOneBased, int totalRepetitions, long warcsCompleted, long warcsTotal,
      long overallStartMs) {
    ProgressServer.Progress p = new ProgressServer.Progress();
    p.status = "running";
    p.currentThreadCount = threadCount;
    p.threadCounts = allThreadCounts;
    p.currentRepetition = repetitionOneBased;
    p.totalRepetitions = totalRepetitions;
    p.warcsCompleted = (int) warcsCompleted;
    p.warcsTotal = (int) warcsTotal;
    p.elapsedMs = System.currentTimeMillis() - overallStartMs;
    return p;
  }

  private static long sumGcCollectionTime() {
    long total = 0;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long t = bean.getCollectionTime();
      if (t >= 0) {
        total += t;
      }
    }
    return total;
  }

  /** Polls {@code com.sun.management.OperatingSystemMXBean.getCpuLoad()} in the background. */
  private static class CpuLoadSampler {
    private final List<Double> samples = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean running = true;
    private Thread thread;
    private final java.lang.management.OperatingSystemMXBean osBean =
        ManagementFactory.getOperatingSystemMXBean();
    private final boolean available =
        osBean instanceof com.sun.management.OperatingSystemMXBean;

    void start() {
      if (!available) {
        return;
      }
      thread = new Thread(() -> {
        com.sun.management.OperatingSystemMXBean sunBean =
            (com.sun.management.OperatingSystemMXBean) osBean;
        while (running) {
          double load = sunBean.getCpuLoad();
          if (load >= 0) { // -1 (or NaN pre-first-call on some JVMs) means "not available yet"
            samples.add(load * 100.0);
          }
          try {
            Thread.sleep(CPU_SAMPLE_INTERVAL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "reindexbench-cpu-sampler");
      thread.setDaemon(true);
      thread.start();
    }

    void stopAndJoin() {
      if (!available) {
        return;
      }
      running = false;
      try {
        thread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    /** Null if the platform CPU-load bean was unavailable on this JVM. */
    Double averageLoadPercent() {
      if (!available || samples.isEmpty()) {
        return available ? null : null;
      }
      return samples.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }
  }

  // *********************************************************************************************
  // * SUMMARY STATISTICS
  // *********************************************************************************************

  private static ReindexBenchReport.SweepPointResult summarizeSweepPoint(
      int threadCount, List<ReindexBenchReport.RepetitionResult> reps) {

    ReindexBenchReport.SweepPointResult sp = new ReindexBenchReport.SweepPointResult();
    sp.threadCount = threadCount;
    sp.repetitions = reps;

    sp.wallClockMs = minMedianMax(reps.stream().map(r -> (double) r.wallClockMs).collect(Collectors.toList()));
    sp.recordsPerSec = minMedianMax(reps.stream().map(r -> r.recordsPerSec).collect(Collectors.toList()));
    sp.mbPerSec = minMedianMax(reps.stream().map(r -> r.mbPerSec).collect(Collectors.toList()));
    sp.aggregateWorkerCpuTimeMs =
        minMedianMax(reps.stream().map(r -> (double) r.aggregateWorkerCpuTimeMs).collect(Collectors.toList()));
    sp.gcTimeMs = minMedianMax(reps.stream().map(r -> (double) r.gcTimeMs).collect(Collectors.toList()));

    List<Double> cpuLoadSamples = reps.stream()
        .map(r -> r.avgSystemCpuLoadPercent)
        .filter(v -> v != null)
        .collect(Collectors.toList());
    sp.avgSystemCpuLoadPercent = cpuLoadSamples.isEmpty() ? null : minMedianMax(cpuLoadSamples);

    List<Double> perWarcTimes = reps.stream()
        .flatMap(r -> r.warcs.stream())
        .filter(w -> w.error == null)
        .map(w -> (double) w.wallClockMs)
        .collect(Collectors.toList());
    sp.perWarcTimeMs = perWarcTimes.isEmpty() ? null : minMedianMax(perWarcTimes);

    sp.failures = reps.stream()
        .flatMap(r -> r.warcs.stream())
        .filter(w -> w.error != null)
        .collect(Collectors.toList());

    sp.warnings = reps.stream()
        .flatMap(r -> r.warcs.stream())
        .filter(w -> w.error == null && w.warning != null)
        .collect(Collectors.toList());

    return sp;
  }

  private static void computeSpeedups(List<ReindexBenchReport.SweepPointResult> sweepResults) {
    ReindexBenchReport.SweepPointResult baseline = sweepResults.stream()
        .filter(sp -> sp.threadCount == 1)
        .findFirst()
        .orElse(null);

    if (baseline == null) {
      return;
    }

    double baselineMedianMs = baseline.wallClockMs.median;

    for (ReindexBenchReport.SweepPointResult sp : sweepResults) {
      if (sp.wallClockMs.median > 0) {
        sp.speedupVsBaseline = baselineMedianMs / sp.wallClockMs.median;
        sp.parallelEfficiency = sp.speedupVsBaseline / sp.threadCount;
      }
    }
  }

  private static ReindexBenchReport.MinMedianMax minMedianMax(List<Double> values) {
    List<Double> sorted = values.stream().sorted().collect(Collectors.toList());
    int n = sorted.size();
    double median = (n % 2 == 1) ?
        sorted.get(n / 2) :
        (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    return new ReindexBenchReport.MinMedianMax(sorted.get(0), median, sorted.get(n - 1));
  }
}
