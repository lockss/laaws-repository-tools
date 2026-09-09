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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Optional, genuinely minimal local HTTP server (JDK built-in {@link HttpServer} only - no web
 * framework dependency) that exposes live sweep progress and, once the sweep finishes, the final
 * report - both as JSON for programmatic use and as a single embedded HTML page (vanilla JS,
 * fully offline, no CDN dependency) that polls progress and renders the results table/chart when
 * done. This is a "neat to have" for a dev-machine operator watching a long sweep run, never a
 * requirement for the tool's core function: the JSON report file written via {@code --out} is
 * always the authoritative record of a run, with or without this server.
 * <p>
 * Progress state is published from the sweep's own execution threads into a single
 * {@link AtomicReference} holding an immutable {@link Progress} snapshot, and read by whichever
 * HTTP handler thread happens to be serving a request - the standard safe-publication pattern for
 * "many readers, occasional writer" shared state, with no locking needed on either side.
 */
public class ProgressServer {
  private static final L4JLogger log = L4JLogger.getLogger();

  public static final int DEFAULT_PORT = 8090;

  private static final ObjectMapper mapper = new ObjectMapper();

  private final AtomicReference<Progress> progressRef =
      new AtomicReference<>(Progress.notStarted());
  private final AtomicReference<ReindexBenchReport.Report> reportRef = new AtomicReference<>();
  private HttpServer server;

  /** Immutable snapshot of sweep progress at one point in time. Public fields: internal, JSON-only. */
  public static class Progress {
    public String status; // "not-started" | "running" | "done"
    public Integer currentThreadCount;
    public List<Integer> threadCounts;
    public Integer currentRepetition; // 1-based
    public Integer totalRepetitions;
    public Integer warcsCompleted;
    public Integer warcsTotal;
    public long elapsedMs;

    static Progress notStarted() {
      Progress p = new Progress();
      p.status = "not-started";
      return p;
    }

    static Progress done(long elapsedMs) {
      Progress p = new Progress();
      p.status = "done";
      p.elapsedMs = elapsedMs;
      return p;
    }
  }

  public void publish(Progress p) {
    progressRef.set(p);
  }

  public void publishDone(long elapsedMs) {
    progressRef.set(Progress.done(elapsedMs));
  }

  public void publishFinalReport(ReindexBenchReport.Report report) {
    reportRef.set(report);
  }

  /** Starts the server on the given port (0 for OS-assigned - not used by the CLI, but handy for tests). */
  public int start(int port) throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/", this::serveIndex);
    server.createContext("/progress", this::serveProgress);
    server.createContext("/report.json", this::serveReportJson);
    server.createContext("/report.html", this::serveReportHtml);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
    int boundPort = server.getAddress().getPort();
    log.info("Reindex-parse benchmark progress server listening on http://localhost:{}/", boundPort);
    System.out.println("Progress server listening on http://localhost:" + boundPort + "/");
    return boundPort;
  }

  public void stop() {
    if (server != null) {
      server.stop(0);
    }
  }

  private void serveIndex(HttpExchange exchange) throws IOException {
    respond(exchange, 200, "text/html; charset=utf-8", INDEX_HTML.getBytes(StandardCharsets.UTF_8));
  }

  private void serveProgress(HttpExchange exchange) throws IOException {
    byte[] body = mapper.writeValueAsBytes(progressRef.get());
    respond(exchange, 200, "application/json; charset=utf-8", body);
  }

  private void serveReportJson(HttpExchange exchange) throws IOException {
    ReindexBenchReport.Report report = reportRef.get();
    if (report == null) {
      byte[] body = "{\"error\":\"Report not available yet - sweep still running\"}"
          .getBytes(StandardCharsets.UTF_8);
      respond(exchange, 404, "application/json; charset=utf-8", body);
      return;
    }
    byte[] body = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(report);
    exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"report.json\"");
    respond(exchange, 200, "application/json; charset=utf-8", body);
  }

  /**
   * Serves the SAME static-HTML rendering ({@link ReindexBenchReportHtml#render}) that
   * {@code ReindexParseBenchmark.main} always writes to disk - there is exactly one
   * report-to-HTML rendering implementation, used identically by both the always-on static file
   * writer and this optional live-viewing server.
   */
  private void serveReportHtml(HttpExchange exchange) throws IOException {
    ReindexBenchReport.Report report = reportRef.get();
    if (report == null) {
      byte[] body = ("<html><body><p>Report not available yet - sweep still running. "
          + "See <a href=\"/\">/</a> for live progress.</p></body></html>")
          .getBytes(StandardCharsets.UTF_8);
      respond(exchange, 404, "text/html; charset=utf-8", body);
      return;
    }
    byte[] body = ReindexBenchReportHtml.render(report, "/report.json")
        .getBytes(StandardCharsets.UTF_8);
    respond(exchange, 200, "text/html; charset=utf-8", body);
  }

  private void respond(HttpExchange exchange, int status, String contentType, byte[] body)
      throws IOException {
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(status, body.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(body);
    }
  }

  // ***********************************************************************************************
  // * EMBEDDED HTML PAGE - vanilla JS only, no external libraries/CDNs, must work fully offline.
  // ***********************************************************************************************

  // This live-progress page ONLY handles the "sweep still running" view: a simple JS poll of
  // /progress every 1.5s. Once the sweep completes it redirects to /report.html, which is
  // rendered by the exact same ReindexBenchReportHtml.render() call that ReindexParseBenchmark's
  // always-on static-file writer uses - there is deliberately no second, JS-side reimplementation
  // of "what a report looks like" here.
  private static final String INDEX_HTML = "<!doctype html>\n"
      + "<html><head><meta charset=\"utf-8\">\n"
      + "<title>Reindex-Parse Benchmark</title>\n"
      + "<style>\n"
      + "  body { font-family: -apple-system, Helvetica, Arial, sans-serif; margin: 2em; color: #222; }\n"
      + "  h1 { font-size: 1.4em; }\n"
      + "  #progressBox { border: 1px solid #ccc; padding: 1em; max-width: 32em; }\n"
      + "  .muted { color: #777; }\n"
      + "</style>\n"
      + "</head><body>\n"
      + "<h1>Reindex-Parse Benchmark</h1>\n"
      + "<div id=\"progressBox\"><p class=\"muted\">Waiting for sweep to start...</p></div>\n"
      + "<script>\n"
      + "function fmtMs(ms) {\n"
      + "  if (ms == null) return '?';\n"
      + "  var s = ms / 1000;\n"
      + "  if (s < 60) return s.toFixed(1) + 's';\n"
      + "  var m = Math.floor(s / 60); s = s - m * 60;\n"
      + "  return m + 'm ' + s.toFixed(0) + 's';\n"
      + "}\n"
      + "function renderProgress(p) {\n"
      + "  var box = document.getElementById('progressBox');\n"
      + "  if (p.status === 'not-started') {\n"
      + "    box.innerHTML = '<p class=\"muted\">Waiting for sweep to start...</p>';\n"
      + "    return;\n"
      + "  }\n"
      + "  var html = '<p><strong>Sweep running</strong> - elapsed ' + fmtMs(p.elapsedMs) + '</p>';\n"
      + "  html += '<p>Thread-count sweep point: <strong>' + p.currentThreadCount + '</strong>';\n"
      + "  if (p.threadCounts) html += ' (of ' + p.threadCounts.join(', ') + ')';\n"
      + "  html += '</p>';\n"
      + "  html += '<p>Repetition: <strong>' + p.currentRepetition + ' / ' + p.totalRepetitions + '</strong></p>';\n"
      + "  html += '<p>WARCs completed this repetition: <strong>' + p.warcsCompleted + ' / ' + p.warcsTotal + '</strong></p>';\n"
      + "  box.innerHTML = html;\n"
      + "}\n"
      + "function poll() {\n"
      + "  fetch('/progress').then(function(r) { return r.json(); }).then(function(p) {\n"
      + "    if (p.status === 'done') { window.location.href = '/report.html'; return; }\n"
      + "    renderProgress(p);\n"
      + "    setTimeout(poll, 1500);\n"
      + "  }).catch(function() { setTimeout(poll, 1500); });\n"
      + "}\n"
      + "poll();\n"
      + "</script>\n"
      + "</body></html>\n";
}
