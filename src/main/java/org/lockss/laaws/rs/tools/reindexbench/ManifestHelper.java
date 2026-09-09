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
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Author-side companion to {@link CorpusManifest}: computes the metadata one manifest entry
 * needs (size, checksum, a cheap record-count estimate) for a single WARC file, or scans a
 * directory of candidate WARCs to help a human pick a diverse corpus before spending any time on
 * checksums.
 * <p>
 * This class does not itself decide what belongs in a corpus - it produces information (a single
 * ready-to-paste manifest entry, or a directory-wide candidate table) for a human to review, trim,
 * tag, and paste into a real manifest by hand.
 */
public class ManifestHelper {
  private static final L4JLogger log = L4JLogger.getLogger();

  public static final int DEFAULT_SAMPLE_RECORDS = 20;

  /** Default namespace for {@code --au} mode, matching {@link BaseLockssRepository#TARGETED_REINDEX_NAMESPACE}
   *  and the convention used everywhere else in this effort. */
  public static final String DEFAULT_NAMESPACE = BaseLockssRepository.TARGETED_REINDEX_NAMESPACE;

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("f", "warc-file", true,
        "Single WARC file to describe; prints one ready-to-paste manifest entry");
    options.addOption(null, "id", true,
        "Manifest entry id to use (single-file mode only; default derived from the file name)");
    options.addOption(null, "tags", true,
        "Comma-separated tags to put in the emitted entry (single-file mode only)");
    options.addOption(null, "scan-candidates", true,
        "Recursively scan a directory for .warc/.warc.gz files and print a candidate table "
            + "(does not read whole files - see --sample-records)");
    options.addOption(null, "sample-records", true,
        "Number of records to sample from the start of each file when estimating record counts "
            + "(default " + DEFAULT_SAMPLE_RECORDS + ")");
    options.addOption(null, "candidates-out", true,
        "With --scan-candidates: also write the scanned candidates as unfiltered, "
            + "manifest-entry-shaped JSON stubs (no sha256 yet) to this file");
    options.addOption(null, "au", true,
        "AUID to build a complete, ready-to-use corpus manifest for (all its WARCs, across all "
            + "--base-paths that contain it). Requires --base-paths and --manifest-out.");
    options.addOption(null, "base-paths", true,
        "Comma-separated list of content base path roots to search for --au's AU directory "
            + "(every base path is checked independently; an AU may live under any or all of them)");
    options.addOption(null, "namespace", true,
        "Namespace the --au AU belongs to (default \"" + DEFAULT_NAMESPACE + "\")");
    options.addOption(null, "manifest-out", true,
        "With --au: path to write (or, if it already exists and parses as a valid manifest, "
            + "append this AU's entries to) a complete CorpusManifest JSON");
    options.addOption(null, "skip-checksums", false,
        "With --au: skip sha256 computation for a fast preview (default for --au mode is to "
            + "compute real checksums, since authoring a manifest is a one-time cost per AU)");
    options.addOption("h", "help", false, "Print this help and exit");

    CommandLine cmd = new DefaultParser().parse(options, args);

    if (cmd.hasOption("help")) {
      new HelpFormatter().printHelp("ManifestHelper", options);
      return;
    }

    int sampleSize = DEFAULT_SAMPLE_RECORDS;
    if (cmd.hasOption("sample-records")) {
      sampleSize = Integer.parseInt(cmd.getOptionValue("sample-records"));
    }

    if (cmd.hasOption("scan-candidates")) {
      scanCandidates(Paths.get(cmd.getOptionValue("scan-candidates")), sampleSize,
          cmd.hasOption("candidates-out") ? Paths.get(cmd.getOptionValue("candidates-out")) : null);
      return;
    }

    List<String> tags = cmd.hasOption("tags") ?
        Arrays.stream(cmd.getOptionValue("tags").split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList()) :
        new ArrayList<>();

    if (cmd.hasOption("au")) {
      if (!cmd.hasOption("base-paths")) {
        throw new ParseException("--base-paths is required with --au");
      }
      if (!cmd.hasOption("manifest-out")) {
        throw new ParseException("--manifest-out is required with --au");
      }

      String auid = cmd.getOptionValue("au");
      List<Path> basePaths = Arrays.stream(cmd.getOptionValue("base-paths").split(","))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .map(Paths::get)
          .collect(Collectors.toList());
      String namespace = cmd.hasOption("namespace") ? cmd.getOptionValue("namespace") : DEFAULT_NAMESPACE;
      Path manifestOut = Paths.get(cmd.getOptionValue("manifest-out"));

      buildAuManifest(auid, basePaths, namespace, manifestOut, tags,
          cmd.hasOption("skip-checksums"), sampleSize);
      return;
    }

    String warcFileArg = cmd.hasOption("warc-file") ? cmd.getOptionValue("warc-file") :
        (cmd.getArgList().isEmpty() ? null : cmd.getArgList().get(0));

    if (warcFileArg == null) {
      new HelpFormatter().printHelp("ManifestHelper", options);
      System.exit(1);
      return;
    }

    describeSingleFile(Paths.get(warcFileArg), cmd.getOptionValue("id"), tags, sampleSize);
  }

  /**
   * Computes size, sha256, compression, and an estimated record-count hint for a single WARC
   * file, and prints a ready-to-paste {@link CorpusManifest.Entry} as JSON.
   * <p>
   * {@code relativePath} in the emitted entry is just the file's own name: this tool has no way
   * to know what a caller's eventual {@code --corpus-dir} will be, so the operator is expected to
   * adjust the path (or move the file under their corpus dir first).
   */
  static void describeSingleFile(Path warcFile, String idOverride, List<String> tags, int sampleSize)
      throws IOException {
    if (!Files.isRegularFile(warcFile)) {
      throw new IOException("Not a regular file: " + warcFile);
    }

    boolean compressed = isCompressedWarcName(warcFile);
    long sizeBytes = Files.size(warcFile);

    log.info("Computing sha256 of {} ({} bytes)...", warcFile, sizeBytes);
    String sha256 = WarcCorpusStats.sha256(warcFile);

    WarcCorpusStats.SampleResult sample;
    try {
      sample = WarcCorpusStats.sample(warcFile, compressed, sampleSize);
    } catch (Exception e) {
      log.warn("Could not sample records from {}, leaving recordCountHint unset", warcFile, e);
      sample = null;
    }

    String id = idOverride != null ? idOverride : deriveId(warcFile);

    Long recordCountHint = (sample != null && sample.estimatedTotalRecords >= 0) ?
        sample.estimatedTotalRecords : null;

    CorpusManifest.Entry entry = new CorpusManifest.Entry(
        id, warcFile.getFileName().toString(), sizeBytes, sha256, compressed, recordCountHint, tags);

    System.out.println(prettyWriter.writeValueAsString(entry));

    if (recordCountHint == null) {
      System.err.println("# NOTE: recordCountHint omitted - could not cheaply estimate a record "
          + "count for this file (fewer than 2 sampled records).");
    } else {
      System.err.println("# NOTE: recordCountHint is a PLACEHOLDER ESTIMATE extrapolated from the "
          + "first " + sampleSize + " records only (avg " + String.format("%.0f", sample.estimatedAvgRecordBytes)
          + " on-disk bytes/record) - it is not an exact count.");
    }
    System.err.println("# NOTE: relativePath is just this file's name; edit it to reflect where "
        + "the file will actually live under your --corpus-dir.");
  }

  /**
   * Builds a complete, ready-to-use {@link CorpusManifest} for every non-metadata WARC belonging
   * to one AU, given its AUID, by resolving the AU's real on-disk directory under each of the
   * given base paths via the actual production naming convention ({@link
   * LocalWarcArtifactDataStore#generateAUPath}), not a reimplementation of it.
   * <p>
   * Unlike {@link #describeSingleFile} and {@link #scanCandidates}, this mode produces no
   * ambiguity for a human to resolve afterward: "every non-metadata WARC belonging to this AU" is
   * a well-defined set, so the manifest this writes is complete and immediately usable, not a
   * candidate list.
   */
  static void buildAuManifest(String auid, List<Path> basePaths, String namespace, Path manifestOut,
                               List<String> tags, boolean skipChecksums, int sampleSize) throws IOException {
    if (basePaths.isEmpty()) {
      throw new IOException("--base-paths must name at least one directory");
    }

    // Throwaway data store, pointed at a scratch directory that has nothing to do with any of the
    // caller's real --base-paths, purely so we can call the real (public) generateAUPath(...)
    // instance method rather than reimplementing its au-<md5(auid)> naming convention ourselves -
    // see ReindexParseBenchmark, which does the same thing for the same reason.
    Path scratchBase = Files.createTempDirectory("manifest-helper-au-scratch");
    LocalWarcArtifactDataStore store;
    try {
      store = new LocalWarcArtifactDataStore(scratchBase);
    } finally {
      deleteRecursively(scratchBase);
    }

    // Check every supplied base path independently - an AU's directory can exist under any or
    // all of them, since nothing shards by AUID.
    List<Path> auDirs = new ArrayList<>();
    Map<Path, Path> checkedPaths = new LinkedHashMap<>();
    for (Path basePath : basePaths) {
      Path auPath = store.generateAUPath(basePath, namespace, auid);
      checkedPaths.put(basePath, auPath);
      if (Files.isDirectory(auPath)) {
        auDirs.add(auPath);
      }
    }

    if (auDirs.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("No AU directory found for AUID=\"").append(auid).append("\", namespace=\"")
          .append(namespace).append("\" under any of the ").append(basePaths.size())
          .append(" supplied --base-paths:");
      for (Map.Entry<Path, Path> e : checkedPaths.entrySet()) {
        sb.append("\n  ").append(e.getKey()).append(" -> ").append(e.getValue())
            .append(" (not a directory)");
      }
      throw new IOException(sb.toString());
    }

    // Recursively find .warc/.warc.gz files under each found AU directory, excluding
    // .metadata.warc(.gz) journal/bookkeeping files - never valid reindex-benchmark content, and
    // not configurable off (a correctness rule, not a preference).
    List<Path> warcFiles = new ArrayList<>();
    for (Path auDir : auDirs) {
      try (Stream<Path> walk = Files.walk(auDir)) {
        walk.filter(Files::isRegularFile)
            .filter(ManifestHelper::isWarcName)
            .filter(p -> !isMetadataWarcName(p))
            .sorted()
            .forEach(warcFiles::add);
      }
    }

    Map<String, Path> idSources = new LinkedHashMap<>();
    List<CorpusManifest.Entry> newEntries = new ArrayList<>();
    long totalBytes = 0;

    for (Path warcFile : warcFiles) {
      boolean compressed = isCompressedWarcName(warcFile);
      long sizeBytes = Files.size(warcFile);
      totalBytes += sizeBytes;

      String sha256;
      if (skipChecksums) {
        // Explicit sentinel, never a fabricated-looking real hash: empty string is clearly not a
        // sha256 digest, whereas any hex-looking placeholder could be mistaken for a real one.
        sha256 = "";
      } else {
        log.info("Computing sha256 of {} ({} bytes)...", warcFile, sizeBytes);
        sha256 = WarcCorpusStats.sha256(warcFile);
      }

      WarcCorpusStats.SampleResult sample;
      try {
        sample = WarcCorpusStats.sample(warcFile, compressed, sampleSize);
      } catch (Exception e) {
        log.warn("Could not sample records from {}, leaving recordCountHint unset", warcFile, e);
        sample = null;
      }
      Long recordCountHint = (sample != null && sample.estimatedTotalRecords >= 0) ?
          sample.estimatedTotalRecords : null;

      String id = deriveId(warcFile);
      if (idSources.containsKey(id)) {
        throw new IOException("Id collision building manifest for AUID=\"" + auid + "\": id \""
            + id + "\" was derived from both\n  " + idSources.get(id) + "\nand\n  " + warcFile
            + "\n(this can happen when an AU's WARCs are split across multiple --base-paths with "
            + "colliding filenames) - rename one of the source files, or build separate manifests "
            + "and merge them by hand.");
      }
      idSources.put(id, warcFile);

      // relativePath is this WARC's absolute, real on-disk path (not relative to any
      // --corpus-dir): Path.resolve() against an absolute path ignores the base and returns the
      // absolute path unchanged, so this resolves correctly at benchmark run time no matter what
      // --corpus-dir is supplied - appropriate here since an AU's WARCs already live at a fixed
      // real location that was never meant to be copied into a per-run corpus-dir.
      CorpusManifest.Entry entry = new CorpusManifest.Entry(
          id, warcFile.toAbsolutePath().normalize().toString(), sizeBytes, sha256, compressed,
          recordCountHint, new ArrayList<>(tags));
      newEntries.add(entry);
    }

    // Load an existing manifest to append to, or start a fresh one.
    CorpusManifest manifest;
    boolean isAppend = Files.exists(manifestOut);
    if (isAppend) {
      manifest = mapper.readValue(manifestOut.toFile(), CorpusManifest.class);
      if (manifest.getEntries() == null) {
        manifest.setEntries(new ArrayList<>());
      }
    } else {
      manifest = new CorpusManifest();
      manifest.setCreatedAt(Instant.now().toString());
      manifest.setNotes("Generated by ManifestHelper --au mode.");
    }

    Map<String, CorpusManifest.Entry> existingById = new LinkedHashMap<>();
    for (CorpusManifest.Entry e : manifest.getEntries()) {
      existingById.put(e.getId(), e);
    }

    for (CorpusManifest.Entry e : newEntries) {
      CorpusManifest.Entry conflict = existingById.get(e.getId());
      if (conflict != null) {
        throw new IOException("Id collision appending to existing manifest " + manifestOut
            + ": id \"" + e.getId() + "\" already present (relativePath=" + conflict.getRelativePath()
            + ") and would be overwritten by a new entry generated for AUID=\"" + auid
            + "\" (relativePath=" + e.getRelativePath() + "). Refusing to overwrite - rename the "
            + "conflicting entry or write to a different --manifest-out.");
      }
    }

    manifest.getEntries().addAll(newEntries);
    prettyWriter.writeValue(manifestOut.toFile(), manifest);

    System.out.println("=== AU manifest summary ===");
    System.out.println("AUID: " + auid);
    System.out.println("Namespace: " + namespace);
    System.out.println("Base paths checked:");
    for (Map.Entry<Path, Path> e : checkedPaths.entrySet()) {
      boolean found = auDirs.contains(e.getValue());
      System.out.println("  " + e.getKey() + " -> " + e.getValue() + (found ? "  [FOUND]" : "  [not present]"));
    }
    System.out.println("WARC files included: " + warcFiles.size());
    System.out.println("Total bytes: " + totalBytes);
    System.out.println("Checksum mode: " + (skipChecksums ?
        "skipped (--skip-checksums; sha256 left as \"\")" : "computed (real sha256 for every entry)"));
    System.out.println("Manifest written to: " + manifestOut
        + (isAppend ? " (appended " + newEntries.size() + " entr" + (newEntries.size() == 1 ? "y" : "ies")
            + " to existing manifest, now " + manifest.getEntries().size() + " total)"
            : " (new manifest, " + newEntries.size() + " entr" + (newEntries.size() == 1 ? "y" : "ies") + ")"));

    if (warcFiles.isEmpty()) {
      System.out.println("# NOTE: the AU directory(ies) below were found but contained no "
          + "non-metadata .warc/.warc.gz files - manifest written with 0 new entries for this AU:");
      auDirs.forEach(d -> System.out.println("  " + d));
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(root)) {
      walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.delete(p);
        } catch (IOException e) {
          log.warn("Could not delete scratch path {}", p, e);
        }
      });
    }
  }

  /**
   * Recursively scans a directory for {@code .warc}/{@code .warc.gz} files and prints a candidate
   * table (path, size, compression, sampled record count, estimated average record size,
   * estimated total record count) without fully reading any of them - only the first
   * {@code sampleRecords} records of each file are read.
   */
  static void scanCandidates(Path directory, int sampleRecords, Path candidatesOut) throws IOException {
    if (!Files.isDirectory(directory)) {
      throw new IOException("Not a directory: " + directory);
    }

    List<Path> candidates;
    try (Stream<Path> walk = Files.walk(directory)) {
      candidates = walk
          .filter(Files::isRegularFile)
          .filter(ManifestHelper::isWarcName)
          .sorted()
          .collect(Collectors.toList());
    }

    if (candidates.isEmpty()) {
      System.out.println("No .warc/.warc.gz files found under " + directory);
      return;
    }

    System.out.printf("%-60s %14s %10s %10s %20s %16s%n",
        "path (relative to " + directory + ")", "sizeBytes", "compressed", "sampled", "estAvgRecBytes",
        "estTotalRecords");

    List<CorpusManifest.Entry> stubs = new ArrayList<>();

    for (Path warcFile : candidates) {
      Path rel = directory.relativize(warcFile);
      boolean compressed = isCompressedWarcName(warcFile);

      WarcCorpusStats.SampleResult sample;
      try {
        sample = WarcCorpusStats.sample(warcFile, compressed, sampleRecords);
      } catch (Exception e) {
        log.warn("Could not sample {}, skipping in candidate table", warcFile, e);
        continue;
      }

      System.out.printf("%-60s %14d %10s %10d %20s %16s%n",
          rel, sample.sizeBytes, compressed, sample.sampledRecordCount,
          sample.estimatedAvgRecordBytes >= 0 ? String.format("%.0f", sample.estimatedAvgRecordBytes) : "n/a",
          sample.estimatedTotalRecords >= 0 ? String.valueOf(sample.estimatedTotalRecords) : "n/a");

      Long recordCountHint = sample.estimatedTotalRecords >= 0 ? sample.estimatedTotalRecords : null;

      stubs.add(new CorpusManifest.Entry(
          deriveId(warcFile), rel.toString(), sample.sizeBytes, null, compressed, recordCountHint,
          new ArrayList<>()));
    }

    System.out.println();
    System.out.println("# These are UNFILTERED candidates, not a manifest: review, trim, tag, and");
    System.out.println("# compute real checksums (via ManifestHelper's single-file mode) only for");
    System.out.println("# the entries you actually keep.");
    System.out.println("# estTotalRecords is extrapolated from the first " + sampleRecords
        + " records of each file only - a rough sizing aid, not a fact about the file.");

    if (candidatesOut != null) {
      List<StubOut> withoutSha = stubs.stream().map(StubOut::new).collect(Collectors.toList());
      prettyWriter.writeValue(candidatesOut.toFile(), withoutSha);
      System.out.println();
      System.out.println("Wrote " + stubs.size() + " candidate stub(s) to " + candidatesOut);
    }
  }

  /**
   * Wraps a {@link CorpusManifest.Entry} for candidate-stub JSON output, omitting the
   * {@code sha256} field entirely (as opposed to serializing it as {@code null}) since no
   * checksum has been computed for candidates that haven't been chosen yet.
   */
  private static class StubOut {
    public final String id;
    public final String relativePath;
    public final long sizeBytes;
    public final boolean compressed;
    public final Long recordCountHint;
    public final List<String> tags;

    StubOut(CorpusManifest.Entry e) {
      this.id = e.getId();
      this.relativePath = e.getRelativePath();
      this.sizeBytes = e.getSizeBytes();
      this.compressed = e.isCompressed();
      this.recordCountHint = e.getRecordCountHint();
      this.tags = e.getTags();
    }
  }

  private static String deriveId(Path warcFile) {
    String name = warcFile.getFileName().toString();
    // Strip .warc.gz or .warc
    if (name.toLowerCase().endsWith(".warc.gz")) {
      return name.substring(0, name.length() - ".warc.gz".length());
    }
    if (name.toLowerCase().endsWith(".warc")) {
      return name.substring(0, name.length() - ".warc".length());
    }
    return name;
  }

  private static boolean isCompressedWarcName(Path path) {
    return path.getFileName().toString().toLowerCase().endsWith(".warc.gz");
  }

  private static boolean isWarcName(Path path) {
    String name = path.getFileName().toString().toLowerCase();
    return name.endsWith(".warc") || name.endsWith(".warc.gz");
  }

  /**
   * True for LOCKSS's own {@code .metadata.warc}/{@code .metadata.warc.gz} journal/bookkeeping
   * files - never valid reindex-benchmark content, regardless of mode.
   */
  private static boolean isMetadataWarcName(Path path) {
    String name = path.getFileName().toString().toLowerCase();
    return name.endsWith(".metadata.warc") || name.endsWith(".metadata.warc.gz");
  }

  private ManifestHelper() {
  }
}
