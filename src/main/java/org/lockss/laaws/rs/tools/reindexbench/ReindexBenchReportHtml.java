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

import java.util.List;

/**
 * Renders a {@link ReindexBenchReport.Report} as a single, complete, self-contained static HTML
 * document - no server, no network fetch, no polling JS. This is the ONE place report-to-HTML
 * rendering logic lives: {@link ReindexParseBenchmark#main} calls it to always write a static
 * {@code .html} report file alongside every run's JSON report, and {@link ProgressServer} calls
 * it (with the same report object, once the sweep completes) to serve the identical rendering
 * over HTTP - there is no second, divergent implementation of "what a report looks like."
 * <p>
 * The page is designed to be opened directly from disk in a browser (no server, no process even
 * running) and to print / "Save as PDF" cleanly via its {@code @media print} rules and an
 * in-page button that calls {@code window.print()} - a plain browser API, unaffected by whether
 * the page came from a file:// URL or an HTTP server.
 */
public class ReindexBenchReportHtml {

  private ReindexBenchReportHtml() {
  }

  /**
   * @param report      the finished report to render.
   * @param jsonLinkHref href for a "Download JSON" link/button - a relative filename when writing
   *                     a static file alongside the JSON report, or an absolute server path
   *                     (e.g. {@code /report.json}) when served live. Null to omit the link
   *                     entirely (e.g. no JSON is available at this href).
   */
  public static String render(ReindexBenchReport.Report report, String jsonLinkHref) {
    StringBuilder html = new StringBuilder();
    html.append("<!doctype html>\n<html><head><meta charset=\"utf-8\">\n");
    html.append("<title>Reindex-Parse Benchmark Report</title>\n");
    html.append(STYLE);
    html.append("</head><body>\n");
    html.append("<div id=\"controls\">");
    html.append("<button onclick=\"window.print()\">Print / Save as PDF</button>");
    if (jsonLinkHref != null) {
      html.append("<a href=\"").append(esc(jsonLinkHref)).append("\" download=\"report.json\">")
          .append("<button>Download JSON</button></a>");
    }
    html.append("</div>\n");

    html.append("<h1>Reindex-Parse Benchmark Report</h1>\n");
    html.append("<p class=\"muted\">Generated ").append(esc(report.runMetadata.timestamp))
        .append("</p>\n");

    renderRunMetadata(html, report);
    renderHardwareProfile(html, report);
    renderParameters(html, report);
    renderCorpusSummary(html, report);
    renderResultsTable(html, report);
    renderChart(html, report);
    renderDbProjection(html, report);
    renderFailuresAndWarnings(html, report);
    renderNotes(html, report);

    html.append("</body></html>\n");
    return html.toString();
  }

  private static void renderRunMetadata(StringBuilder html, ReindexBenchReport.Report report) {
    ReindexBenchReport.RunMetadata rm = report.runMetadata;
    html.append("<h2>Run metadata</h2>\n<table>\n");
    row(html, "JVM", rm.jvmVersion);
    row(html, "OS", rm.osName + " " + rm.osVersion);
    row(html, "Available processors", String.valueOf(rm.availableProcessors));
    row(html, "Manifest", rm.manifestPath);
    row(html, "Corpus dir", rm.corpusDir);
    row(html, "Checksum verification", rm.checksumVerification);
    row(html, "lockss-core revision", rm.lockssCoreGitRevision == null ? "(unknown)" : rm.lockssCoreGitRevision);
    html.append("</table>\n");
  }

  private static void renderHardwareProfile(StringBuilder html, ReindexBenchReport.Report report) {
    ReindexBenchReport.HardwareProfile hw = report.runMetadata.hardwareProfile;
    html.append("<h2>Hardware profile</h2>\n<table>\n");
    if (hw == null) {
      row(html, "Status", "not captured");
      html.append("</table>\n");
      return;
    }
    row(html, "Platform", hw.platform);
    row(html, "CPU model", hw.cpuModel);
    row(html, "Physical cores", hw.physicalCores == null ? "unknown" : String.valueOf(hw.physicalCores));
    row(html, "Logical cores", hw.logicalCores == null ? "unknown" : String.valueOf(hw.logicalCores));
    row(html, "Core topology", hw.coreTopology);
    if (hw.performanceCores != null && hw.efficiencyCores != null) {
      row(html, "Performance / efficiency cores",
          hw.performanceCores + " performance + " + hw.efficiencyCores + " efficiency");
    }
    row(html, "Total RAM", hw.totalMemBytes == null ? "unknown" : formatBytes(hw.totalMemBytes));
    row(html, "Storage", hw.storageSummary);
    if (hw.storageDetectionMethod != null) {
      row(html, "Storage detection method", hw.storageDetectionMethod);
    }
    if (hw.notes != null) {
      row(html, "Notes", hw.notes);
    }
    html.append("</table>\n");
  }

  private static String formatBytes(long bytes) {
    double gb = bytes / (1024.0 * 1024.0 * 1024.0);
    return String.format("%,.1f GB (%,d bytes)", gb, bytes);
  }

  private static void renderParameters(StringBuilder html, ReindexBenchReport.Report report) {
    ReindexBenchReport.Parameters p = report.parameters;
    html.append("<h2>Parameters</h2>\n<table>\n");
    row(html, "Tag filter", p.tag);
    row(html, "Threads swept", p.threads.toString());
    row(html, "Repetitions", String.valueOf(p.repetitions));
    row(html, "Verify checksums", String.valueOf(p.verifyChecksums));
    html.append("</table>\n");
  }

  private static void renderCorpusSummary(StringBuilder html, ReindexBenchReport.Report report) {
    ReindexBenchReport.CorpusSummary cs = report.corpusSummary;
    html.append("<h2>Corpus summary</h2>\n<table>\n");
    row(html, "Entry count", String.valueOf(cs.entryCount));
    row(html, "Total bytes", String.format("%,d", cs.totalBytes));
    row(html, "Total record count hint", cs.totalRecordCountHint == null ? "(unknown)" : String.format("%,d", cs.totalRecordCountHint));
    row(html, "Tags present", cs.tagsPresent.toString());
    html.append("</table>\n");
  }

  private static void renderResultsTable(StringBuilder html, ReindexBenchReport.Report report) {
    html.append("<h2>Results (parse-only)</h2>\n");
    html.append("<p class=\"muted\">WARC-parse speedup vs. thread count, measured directly on "
        + "this run's corpus. Does NOT include database-commit time - see \"Projected end-to-end "
        + "reindex speedup\" below.</p>\n");
    html.append("<table>\n");
    html.append("<tr><th>Threads</th><th>Wall-clock (median ms)</th><th>Records/sec</th>"
        + "<th>MB/sec</th><th>Speedup</th><th>Efficiency</th></tr>\n");
    for (ReindexBenchReport.SweepPointResult sp : report.sweepResults) {
      html.append("<tr><td>").append(sp.threadCount).append("</td>");
      html.append("<td>").append(fmt(sp.wallClockMs.median, 0)).append("</td>");
      html.append("<td>").append(fmt(sp.recordsPerSec.median, 1)).append("</td>");
      html.append("<td>").append(fmt(sp.mbPerSec.median, 2)).append("</td>");
      html.append("<td>").append(sp.speedupVsBaseline == null ? "-" : fmt(sp.speedupVsBaseline, 2) + "x").append("</td>");
      html.append("<td>").append(sp.parallelEfficiency == null ? "-" : fmt(sp.parallelEfficiency * 100, 0) + "%").append("</td>");
      html.append("</tr>\n");
    }
    html.append("</table>\n");
  }

  private static void renderChart(StringBuilder html, ReindexBenchReport.Report report) {
    List<ReindexBenchReport.SweepPointResult> results = report.sweepResults;
    if (results.isEmpty()) {
      return;
    }
    double maxSpeedup = 1.0;
    for (ReindexBenchReport.SweepPointResult sp : results) {
      if (sp.speedupVsBaseline != null) {
        maxSpeedup = Math.max(maxSpeedup, sp.speedupVsBaseline);
      }
    }
    html.append("<h2>Speedup vs. thread count</h2>\n");
    int maxBarWidth = 400;
    for (ReindexBenchReport.SweepPointResult sp : results) {
      double speedup = sp.speedupVsBaseline == null ? 0 : sp.speedupVsBaseline;
      int w = Math.max(2, (int) Math.round((speedup / maxSpeedup) * maxBarWidth));
      html.append("<div class=\"bar-row\"><span class=\"bar-label\">").append(sp.threadCount)
          .append("T</span><div class=\"bar\" style=\"width:").append(w).append("px\"></div>")
          .append("<span style=\"margin-left:8px\">").append(fmt(speedup, 2)).append("x</span></div>\n");
    }
  }

  /**
   * Renders the "Projected end-to-end reindex speedup" section: the combined parse+DB projection
   * when {@code --db-benchmark-report} was supplied ({@link ReindexBenchReport.Report#dbProjection}
   * non-null), or a short, clear caveat explaining why the parse-only numbers above overstate true
   * end-to-end speedup when it wasn't. Kept entirely separate from {@link #renderResultsTable} -
   * that table is left exactly as it always was (parse-only), never overwritten or conflated with
   * this section.
   */
  private static void renderDbProjection(StringBuilder html, ReindexBenchReport.Report report) {
    html.append("<h2>Projected end-to-end reindex speedup</h2>\n");

    ReindexBenchReport.DbProjection proj = report.dbProjection;
    if (proj == null) {
      html.append("<p>No <code>--db-benchmark-report</code> was supplied for this run, so this "
          + "section cannot be computed. This report measures WARC-parse speedup ONLY - "
          + "database-commit time is a real fraction of total reindex time NOT accounted for in "
          + "the parse-only results above. Earlier measurement in this project (the WARC-vs-DB "
          + "split investigation) found DB-commit time to be roughly <strong>20-35% of total "
          + "reindex time</strong> for typical real content in the 30KB-400KB average-artifact-"
          + "size range, and that this DB share <strong>shrinks further</strong> as average "
          + "artifact size grows (a large-average-artifact AU is dominated by decompression, "
          + "nearly regardless of DB behavior) - see the \"WARC-read/parse time vs. DB-commit "
          + "time\" addendum in <code>au-reindex-recovery-plan.md</code> for the full measurement."
          + " Consequently, actual end-to-end reindex speedup will be somewhat "
          + "<strong>LOWER</strong> than the parse-only numbers shown above, not equal to them.</p>\n");
      html.append("<p>Pass <code>--db-benchmark-report &lt;path&gt;</code> (from a "
          + "<code>ReindexConcurrencyBenchmark</code> run in lockss-core) to get a combined "
          + "projection instead of this caveat.</p>\n");
      return;
    }

    html.append("<table>\n");
    row(html, "DB benchmark report", proj.dbBenchmarkReportPath);
    row(html, "DB benchmark report timestamp",
        proj.dbBenchmarkReportTimestamp == null ? "(unknown)" : proj.dbBenchmarkReportTimestamp);
    row(html, "This corpus's measured total record count", String.format("%,d", proj.totalRecordCount));
    html.append("</table>\n");

    html.append("<p class=\"muted\">Assumes a simple architecture where each of the K worker "
        + "threads does both parsing AND its own DB commits for the WARCs it is assigned - one "
        + "thread, start to finish, per WARC, the same K controlling both phases. A more "
        + "sophisticated producer/consumer split (more parser threads feeding fewer DB-committer "
        + "threads, since DB commits are cheap after batching) could plausibly do "
        + "<strong>better</strong> than this projection - treat it as a reasonable "
        + "first-implementation-shaped estimate, not a ceiling on what is achievable.</p>\n");
    html.append("<p class=\"muted\">The DB-commit rate below was measured on a "
        + "<strong>different, synthetic corpus</strong> (via <code>ReindexConcurrencyBenchmark</code>)"
        + " and extrapolated here onto this corpus's actual record count by ARTIFACT COUNT, not "
        + "measured on this corpus directly. This is legitimate for DB-commit rate - unlike it "
        + "would be for parse rate - because DB-commit cost is dominated by fixed per-commit "
        + "overhead (WAL fsync, batch flush) largely insensitive to which specific bytes are "
        + "committed, whereas WARC-parse cost is dominated by per-byte decompression and scales "
        + "with corpus bytes, which is exactly why this tool always measures parse time directly "
        + "on the real corpus rather than assuming it transfers across corpora.</p>\n");

    html.append("<table>\n");
    html.append("<tr><th>Parse threads (K)</th><th>DB benchmark K used</th>"
        + "<th>DB artifacts/sec (median)</th><th>Parse wall-clock (median ms)</th>"
        + "<th>Extrapolated DB time (ms)</th><th>Total time (ms)</th>"
        + "<th>Projected speedup</th></tr>\n");
    for (ReindexBenchReport.DbProjectionPoint pt : proj.points) {
      html.append("<tr><td>").append(pt.parseThreadCount).append("</td>");
      html.append("<td>").append(pt.dbBenchmarkKUsed)
          .append(pt.dbBenchmarkKUsed != pt.parseThreadCount ? " (nearest available)" : "")
          .append("</td>");
      html.append("<td>").append(fmt(pt.dbArtifactsPerSecMedian, 1)).append("</td>");
      html.append("<td>").append(fmt(pt.parseWallClockMsMedian, 0)).append("</td>");
      html.append("<td>").append(fmt(pt.extrapolatedDbTimeMs, 0)).append("</td>");
      html.append("<td>").append(fmt(pt.totalTimeMs, 0)).append("</td>");
      html.append("<td>").append(pt.projectedSpeedup == null ? "-" : fmt(pt.projectedSpeedup, 2) + "x").append("</td>");
      html.append("</tr>\n");
    }
    html.append("</table>\n");
  }

  private static void renderFailuresAndWarnings(StringBuilder html, ReindexBenchReport.Report report) {
    boolean anyFailures = report.sweepResults.stream().anyMatch(sp -> !sp.failures.isEmpty());
    boolean anyWarnings = report.sweepResults.stream().anyMatch(sp -> !sp.warnings.isEmpty());

    if (anyFailures) {
      html.append("<h2>Failures</h2>\n<ul>\n");
      for (ReindexBenchReport.SweepPointResult sp : report.sweepResults) {
        for (ReindexBenchReport.WarcOutcome w : sp.failures) {
          html.append("<li>").append(esc(w.entryId)).append(" (").append(esc(w.path)).append("): ")
              .append(esc(w.error)).append("</li>\n");
        }
      }
      html.append("</ul>\n");
    }

    if (anyWarnings) {
      html.append("<h2>Warnings</h2>\n<ul>\n");
      for (ReindexBenchReport.SweepPointResult sp : report.sweepResults) {
        for (ReindexBenchReport.WarcOutcome w : sp.warnings) {
          html.append("<li>").append(esc(w.entryId)).append(" (").append(esc(w.path)).append("): ")
              .append(esc(w.warning)).append("</li>\n");
        }
      }
      html.append("</ul>\n");
    }

    if (!anyFailures && !anyWarnings) {
      html.append("<p>No WARC failures or warnings across the sweep.</p>\n");
    }
  }

  private static void renderNotes(StringBuilder html, ReindexBenchReport.Report report) {
    if (report.notes.isEmpty()) {
      return;
    }
    html.append("<h2>Notes</h2>\n<ul>\n");
    for (String note : report.notes) {
      html.append("<li>").append(esc(note)).append("</li>\n");
    }
    html.append("</ul>\n");
  }

  private static void row(StringBuilder html, String label, String value) {
    html.append("<tr><td>").append(esc(label)).append("</td><td>")
        .append(esc(value == null ? "" : value)).append("</td></tr>\n");
  }

  private static String fmt(double value, int decimals) {
    return String.format("%,." + decimals + "f", value);
  }

  private static String esc(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
  }

  // Dark by default (a standing preference for tool-generated reports, not gated behind
  // prefers-color-scheme - this is a private, single-viewer local file, not a shared Artifact).
  // Print output stays light/white regardless: printing a dark background wastes ink and reads
  // poorly on paper, and the existing @media print rules already assume a light page.
  private static final String STYLE = "<style>\n"
      + "  :root { color-scheme: dark; }\n"
      + "  body { font-family: -apple-system, Helvetica, Arial, sans-serif; margin: 2em;"
      + " background: #14171a; color: #e2e5e8; }\n"
      + "  h1, h2 { color: #f2f4f6; }\n"
      + "  h1 { font-size: 1.4em; }\n"
      + "  a { color: #6cb6ff; }\n"
      + "  table { border-collapse: collapse; margin: 1em 0; }\n"
      + "  th, td { border: 1px solid #3a3f45; padding: 4px 10px; text-align: right;"
      + " font-variant-numeric: tabular-nums; }\n"
      + "  th { background: #22262b; color: #f2f4f6; }\n"
      + "  td:first-child, th:first-child { text-align: left; }\n"
      + "  .bar-row { display: flex; align-items: center; margin: 2px 0; }\n"
      + "  .bar-label { width: 4em; text-align: right; margin-right: 8px; font-variant-numeric: tabular-nums; }\n"
      + "  .bar { height: 16px; background: #4fae7d; }\n"
      + "  #controls button { font-size: 1em; padding: 0.4em 1em; margin-right: 0.5em;"
      + " background: #262b31; color: #e2e5e8; border: 1px solid #454b52; border-radius: 4px;"
      + " cursor: pointer; }\n"
      + "  #controls button:hover { background: #30363d; }\n"
      + "  .muted { color: #9aa1a8; }\n"
      + "  @media print {\n"
      + "    body { margin: 0.5in; background: #fff !important; color: #000; }\n"
      + "    h1, h2 { color: #000; }\n"
      + "    a { color: #000; }\n"
      + "    #controls { display: none !important; }\n"
      + "    table, th, td { -webkit-print-color-adjust: exact; print-color-adjust: exact;"
      + " border-color: #999 !important; }\n"
      + "    th { background: #ddd !important; color: #000 !important; }\n"
      + "    tr { page-break-inside: avoid; }\n"
      + "    h1, h2 { page-break-after: avoid; }\n"
      + "  }\n"
      + "</style>\n";
}
