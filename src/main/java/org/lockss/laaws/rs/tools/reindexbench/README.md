# Reindex-parse benchmark

A standalone command-line tool for measuring how WARC-parsing throughput for artifact reindexing
scales with thread count, using the real production parsing code
(`WarcArtifactDataStore.indexArtifactsFromWarc(...)`) against a throwaway local scratch directory
and an in-memory, no-op counting index - never a real database, never a real repository, never
production data. It is not production code and must never be treated as such.

This tool answers one question: if WARC parsing is split across worker threads (one thread per
WARC file - a single WARC file is always parsed start-to-finish by one thread, never split), how
much wall-clock speedup does that actually buy on your hardware, and at what thread count does it
stop paying off? It measures; it does not decide anything on its own, and it does not change any
production code path.

## Two pieces

- **`ManifestHelper`** - author-side tool. Given a WARC file, computes the size/checksum/rough
  record-count-estimate a manifest entry needs. Given a directory, cheaply scans it for
  `.warc`/`.warc.gz` candidates without fully reading any of them. Given an AUID and one or more
  content base paths, builds a complete, ready-to-use manifest for every WARC belonging to that AU.
- **`ReindexParseBenchmark`** - the benchmark itself. Given a manifest and a local corpus
  directory, verifies the corpus, sweeps a range of thread counts, and writes a JSON report.

## Authoring a corpus manifest

A manifest is a small JSON file listing WARC files by relative path, size, and checksum - it never
contains WARC bytes itself. Each person who runs a benchmark against a given manifest keeps their
own local copy of the corpus under their own `--corpus-dir`; the manifest just describes what
should be there.

### Step 1: find candidates

Point `ManifestHelper` at a directory of WARCs you might want to include:

```
java -cp ... org.lockss.laaws.rs.tools.reindexbench.ManifestHelper \
  --scan-candidates /path/to/some/warcs --candidates-out candidates.json
```

This prints a table (path, size, compression, a sampled record count, and an estimated total
record count) and, if `--candidates-out` is given, writes the same information as unfiltered,
manifest-entry-shaped JSON stubs (no checksum yet). The scan only reads the **first** N records
of each file (default 20, override with `--sample-records`) to estimate an average record size,
then extrapolates a total from file size - it does **not** read any file end to end. This matters
because advancing a compressed WARC reader past a record forces decompression of that record's
full payload regardless of whether anything reads it, so a full scan of a large candidate set
would cost nearly as much as the benchmark itself.

Review the candidate table (or the stub JSON) and decide which files you actually want in your
manifest - by size, by rough average record size (a proxy for "small-artifact AU" vs.
"large-artifact AU" - the axis reindex-parse cost is most sensitive to), and by however much
diversity your sweep needs.

### Step 2: compute real entries for the files you keep

For each WARC you decided to keep, run `ManifestHelper` in single-file mode. This is the one step
that actually reads the whole file (to checksum it):

```
java -cp ... org.lockss.laaws.rs.tools.reindexbench.ManifestHelper \
  --warc-file /path/to/some/warcs/springer-vol16.warc.gz \
  --tags small-avg-artifact \
  --id springer-vol16-small-avg
```

This prints a ready-to-paste manifest entry:

```json
{
  "id" : "springer-vol16-small-avg",
  "relativePath" : "springer-vol16.warc.gz",
  "sizeBytes" : 214748364,
  "sha256" : "...",
  "compressed" : true,
  "recordCountHint" : 1656,
  "tags" : [ "small-avg-artifact" ]
}
```

Paste it into your manifest's `entries` array (see the schema below), and fix up `relativePath`
to reflect where the file actually lives under your intended `--corpus-dir` - `ManifestHelper`
only knows the file's own name, not your corpus layout.

`recordCountHint` is always an estimate (extrapolated from a small sample, exactly as in the scan
step above), never an exact count - `ManifestHelper` deliberately does not parse a whole WARC just
to count its records.

### Alternative: build a complete manifest for one whole AU

If you want every WARC belonging to one Archival Unit (not a hand-picked subset), skip Steps 1-2
above and use `ManifestHelper`'s `--au` mode instead. Given an AUID, it resolves the AU's real
on-disk directory the same way production does (`WarcArtifactDataStore.generateAUPath`, reused
directly rather than reimplemented), checks every base path you supply independently (an AU can
live under any or all of them - nothing shards by AUID), and writes a complete, ready-to-use
manifest with no further curation needed:

```
java -cp ... org.lockss.laaws.rs.tools.reindexbench.ManifestHelper \
  --au "org|lockss|plugin|...|volume_name~16" \
  --base-paths /content0,/content1,/content2 \
  --namespace lockss \
  --manifest-out au-manifest.json \
  --tags some-tag,another-tag
```

- Finds every `.warc`/`.warc.gz` under the AU's directory, excluding `.metadata.warc` journal files
  (never valid benchmark content).
- Unlike single-file/scan-candidates mode, entries' `relativePath` here is the WARC's real absolute
  on-disk path, not a name to fix up under a `--corpus-dir` - an AU's files already live at a fixed
  location that was never meant to be copied. (`Path.resolve()` against an absolute path returns it
  unchanged, so this works correctly with whatever `--corpus-dir` you pass `ReindexParseBenchmark`.)
- Computes real sha256 checksums by default (unlike the benchmark's own default) since authoring a
  manifest is a one-time cost per AU; pass `--skip-checksums` for a fast preview (`sha256` is then
  left as `""`, never a fabricated-looking placeholder).
- If `--manifest-out` already exists and parses as a manifest, new entries are **appended** to it
  (run repeatedly with different `--au` values to build up a multi-AU manifest); an `id` collision,
  either within one run (rare, but possible if the AU's WARCs span base paths with colliding
  filenames) or against the existing file, fails loudly naming both entries rather than silently
  overwriting.
- If the AU isn't found under any supplied base path, fails loudly naming the AUID, namespace, and
  every base path checked - never silently writes an empty manifest.

### Manifest schema

```json
{
  "manifestVersion": 1,
  "createdAt": "2026-08-26T00:00:00Z",
  "notes": "free-text",
  "entries": [
    {
      "id": "springer-vol16-small-avg",
      "relativePath": "springer/artifacts.warc.gz",
      "sizeBytes": 214748364,
      "sha256": "...",
      "compressed": true,
      "recordCountHint": 1656,
      "tags": ["small-avg-artifact"]
    }
  ]
}
```

Before every run, `ReindexParseBenchmark` resolves every selected entry's `relativePath` against
`--corpus-dir` and verifies its size against the manifest; a size mismatch or missing file fails
the run loudly and immediately - a benchmark over silently wrong input is worse than no benchmark.

Checksum verification is **off by default** - pass `--verify-checksums` to opt in when you
specifically want to confirm your own repeatedly-used local corpus copy hasn't drifted since it
was checksummed. It defaults off because it only ever protects against that one narrow case (see
below), and computing sha256 over a manifest's worth of large WARCs is real time most runs don't
need to spend. The report always records plainly whether verification ran
(`"checksumVerification": "verified"` or `"skipped"`) so a report never leaves that unstated -
"skipped" is the normal, expected value, not a warning sign.

**On the checksum's actual purpose:** it exists only to catch drift or corruption in **your own**
local copy of the corpus between two of **your own** runs. WARCs produced by independent crawls of
nominally "the same" content are essentially never byte-identical in practice - crawl timing,
crawl order, and upstream content changes (even something as small as a response `Date` header)
guarantee differences. Two operators' local copies of a corpus described by the same manifest
`id` will not, and are not expected to, hash the same. Never treat a matching manifest `id`
across two people's reports as proof they measured identical bytes; compare using the corpus
statistics recorded in each report instead (entry count, total bytes, tag breakdown).

## Running a sweep

```
java -cp ... org.lockss.laaws.rs.tools.reindexbench.ReindexParseBenchmark \
  --manifest corpus-manifest.json --corpus-dir /path/to/local/corpus \
  --threads 1,2,4,8,16,32 --repetitions 3 --tag small-avg-artifact \
  --out report-small-avg.json
```

Omit `--tag` to run against the whole manifest (every entry, regardless of tag).

Each thread count in `--threads` (default: `1,2,4,8,{cores},{cores*2},{cores*4}`, computed at
runtime and deliberately swept past your core count) is run `--repetitions` times (default 3)
over the full selected corpus. `1` is always included - it is the sweep's baseline row, and every
other row's speedup and parallel efficiency are computed against its median wall-clock time.
Never trust a single-shot number: every metric in the report is a median with min/max alongside
it, computed across the repetitions actually run.

### Recommended workflow: segmented, then mixed

1. Run one sweep per `--tag` first, to characterize each size class on its own (e.g. a
   `small-avg-artifact` sweep, a `large-avg-artifact` sweep). Reindex-parse cost is dominated by
   decompression, which scales with corpus bytes, not record count - so a corpus of small
   artifacts and a corpus of large artifacts can behave very differently under threading, and
   segmenting first tells you clearly which is which.
2. Once each size class is characterized, run one final **untagged** sweep over the whole
   manifest - the mixed-corpus run that best approximates a real, heterogeneous AU population.

### Corpus size

Start with roughly **a few GB of WARC content per sweep point** (i.e. per `--tag` size class, or
per whole-manifest mixed run). This is a starting guideline, not an enforced limit - the tool will
run a corpus of any size, but a much larger corpus multiplies the time needed to complete a full
sweep (every thread count times every repetition) without changing what the resulting numbers
mean.

## Projected end-to-end reindex speedup

Every report's "Results" table (now explicitly labeled "Results (parse-only)") measures WARC-parse
speedup only. A real reindex also spends time in database commit, which this tool deliberately
never measures itself - `ReindexParseBenchmark` must stay database-free (see its class javadoc).
Without further input, the report instead prints a caveat: DB-commit time was separately measured
(in `au-reindex-recovery-plan.md`'s "WARC-read/parse time vs. DB-commit time" addendum) at roughly
20-35% of total reindex time for typical real content, and that share *shrinks* further as average
artifact size grows - so actual end-to-end speedup will be somewhat lower than the parse-only
numbers shown, not equal to them.

To get a combined projection instead of that caveat, pass:

```
--db-benchmark-report <path-to-a-ReindexConcurrencyBenchmark-report.json>
```

pointing at a JSON report from lockss-core's `ReindexConcurrencyBenchmark`
(`src/test/java/org/lockss/rs/io/index/db/ReindexConcurrencyBenchmark.java`), which measures how
batched DB-commit throughput scales under concurrent writer threads. For each thread count K in
this tool's own `--threads` sweep, the report looks up that DB benchmark's measured
`artifactsPerSecMedian` at the same K (or the nearest K it actually measured, named explicitly -
never a fabricated interpolated value), extrapolates a DB-commit time for this corpus's actual
(measured) record count, and adds it to this tool's own measured parse time to project a combined
speedup. The report states explicitly why extrapolating the DB rate across corpora by artifact
count is reasonable (DB-commit cost is dominated by fixed per-commit overhead, largely insensitive
to which bytes are committed) while doing the same for parse rate would not be (parse cost scales
with corpus bytes, which is exactly why parse is always measured directly here) - and that the
projection assumes a simple one-thread-does-both-phases architecture, so a more sophisticated
producer/consumer split could plausibly do better than what's projected.

A path that doesn't exist, isn't valid JSON, or doesn't look like a `ReindexConcurrencyBenchmark`
report (no `results` array, or a non-positive/non-finite `artifactsPerSecMedian`) fails the run
loudly, naming the path and what's wrong - it never silently falls back to the no-DB-data caveat.

## Recomputing derived output without re-running the sweep

`--regenerate-report <path-to-an-existing-report.json>` recomputes and rewrites only a report's
*derived* output (the "Projected end-to-end reindex speedup" section, and the HTML render) from an
already-produced `ReindexParseBenchmark` JSON report - it executes no sweep and reads no WARCs.
This is different from `--repeat-report`, which re-runs the sweep using a prior report's recorded
parameters: `--regenerate-report` is for when the (potentially expensive) parse sweep already ran
and a `ReindexConcurrencyBenchmark` report only became available afterward, and you want the
combined projection added without paying to re-read the whole corpus again.

```
java -cp ... org.lockss.laaws.rs.tools.reindexbench.ReindexParseBenchmark \
  --regenerate-report old-report.json --db-benchmark-report db-report.json \
  --out old-report-with-projection.json
```

`--db-benchmark-report` is optional even here: `--regenerate-report` alone just re-renders the
JSON/HTML from the existing report's already-measured data unchanged (e.g. useful after an HTML-
template change, with no new data). `--out` is still required, and is never the same file as the
input report - nothing is overwritten in place. `--manifest`/`--corpus-dir`/`--threads`/
`--repetitions`/`--repeat-report`/`--tag`/`--verify-checksums`/`--scratch-dir`/`--lockss-core-dir`
are not applicable in this mode (no sweep runs) and are ignored, with a note printed for each one
supplied, rather than silently changing behavior.

## Reproducing a prior run

```
java -cp ... org.lockss.laaws.rs.tools.reindexbench.ReindexParseBenchmark \
  --repeat-report report-small-avg.json --corpus-dir /path/to/local/corpus \
  --out report-small-avg-2.json
```

This reruns with the prior report's manifest, `--tag` filter, thread list, and repetition count
exactly as recorded - `--corpus-dir` must always be re-supplied explicitly (a manifest is
portable; a local corpus copy is not, and this tool never assumes a path recorded in an old report
still resolves on this machine, or even on the same machine). Checksum verification is inherited
from the prior report's recorded setting; passing `--verify-checksums` on the repeat invocation
turns it on regardless of what the prior report used (there is no flag to force it back off on a
repeat - the default is already off, so simply omit `--verify-checksums`).

## Building and deploying

Run `mvn -o package` in `laaws-repository-tools`. This produces, among other artifacts, a
self-contained executable jar:

```
target/laaws-repository-tools-<version>-reindexbench.jar
```

Copy just that one jar to the dev machine and run it directly - no other files, no classpath
setup, no other jars from this repo needed:

```
java -jar laaws-repository-tools-<version>-reindexbench.jar --manifest corpus-manifest.json \
  --corpus-dir /path/to/local/corpus --out report.json
```

**Pass an explicit `-Xmx`, sized well above the JVM's default.** A real sweep at high thread
counts (8-10+) against a corpus containing individually large WARCs (tens of MB, common in real
repository content) can exhaust the JVM's default ergonomic heap (typically 1/4 of system RAM)
even on a machine with plenty of RAM to spare, because that many threads concurrently
decompressing large records at once genuinely needs more heap headroom than the default assumes.
This surfaces as `OutOfMemoryError`, recorded per-WARC (a benchmark this size does not abort), but
it still costs you those data points. Start with something generous relative to the machine's
RAM, e.g. `-Xmx16g` on a 32 GB machine, and increase it further if `OutOfMemoryError` still shows
up in a run's failures/warnings summary at your higher thread counts:

```
java -Xmx16g -jar laaws-repository-tools-<version>-reindexbench.jar --manifest corpus-manifest.json \
  --corpus-dir /path/to/local/corpus --out report.json
```

`ManifestHelper` is not this jar's default `Main-Class` (`ReindexParseBenchmark` is, since the
sweep is this tool's primary entry point), but it remains runnable from the exact same jar via
`-cp`:

```
java -cp laaws-repository-tools-<version>-reindexbench.jar \
  org.lockss.laaws.rs.tools.reindexbench.ManifestHelper --help
```

This jar is built by a dedicated `maven-shade-plugin` execution (classifier `reindexbench`,
bound to the `package` phase) - separate from, and non-disruptive to, this module's existing
Spring-Boot `repackageJar` execution that produces the `-with-deps` jar for
`SolrArtifactIndexAdmin`. That existing mechanism could not be reused here: a Spring-Boot
repackaged jar's classes live under `BOOT-INF/classes` with dependencies as nested jars, which
cannot satisfy a plain `-cp` classpath lookup (needed for `ManifestHelper` above) or a direct
`Main-Class` manifest entry pointing at `ReindexParseBenchmark` (rather than a Spring Boot loader
shim). The shaded jar is large (several hundred MB, since it bundles Solr/Lucene and the rest of
this module's dependency graph) - that is an expected and accepted cost of "copy one jar, run it
anywhere," not a defect.

## Reading a report

Every report records, alongside the full per-sweep-point results table:

- run metadata: timestamp, JVM/OS info, available processors, the manifest's own checksum, the
  lockss-core git revision if it could be determined (best-effort; a missing revision is noted,
  not treated as an error), and the resolved checksum of every manifest entry actually used;
- the full parameter set used (manifest, corpus-dir, tag filter, thread list, repetitions,
  whether checksum verification ran);
- a corpus summary (entry count, total bytes, estimated total record count, tags present);
- per sweep point: median/min/max wall-clock time, records/sec, MB/sec (the throughput metric
  that's portable across differently-sized-artifact corpora), aggregate worker CPU time, average
  system CPU utilization (when the JVM exposes it), GC time, per-WARC min/median/max time (for
  tail-latency visibility), speedup and parallel efficiency vs. the N=1 baseline, and every WARC
  failure encountered, named explicitly by manifest id and path - never silently dropped;
- an explicit note on what the checksum can and cannot prove (see above).

### Robustness to bad/corrupt WARC files

Before ever calling into the real WARC parser for a file, the tool does a cheap magic-byte check
(gzip `0x1F 0x8B` for a `.warc.gz` entry, ASCII `WARC/` for a `.warc` entry). A file that fails
this check is recorded as that WARC's `outcome.error` and never parsed - this matters because the
underlying JWAT reader treats "no valid WARC records found by EOF" as an empty, successful
iteration rather than throwing, so without this check a file of pure garbage bytes named `*.warc`
would otherwise be silently reported as `recordsIndexed: 0` with no error, indistinguishable from
a genuinely empty valid WARC.

Separately, a WARC that passes the magic-byte check but comes back with zero records indexed gets
a non-fatal `outcome.warning` (surfaced in the JSON report per-WARC, rolled up into each sweep
point's `warnings` list, and printed to the console after the run) whenever the manifest's
`recordCountHint` for that entry was greater than zero - a heads-up that this may be a silent
parse failure worth checking by hand, not a hard failure (a genuinely empty valid WARC is a real,
non-error possibility and is never flagged this way).

Every per-WARC task catches `Throwable`, not just `Exception`: a sufficiently corrupt or
adversarial WARC could plausibly trigger an `Error` (e.g. `OutOfMemoryError` from a bogus length
field), and that must end up as just that one file's `outcome.error` (noting the exception class
name) rather than aborting the entire sweep pass.

## Report output: always JSON + HTML, optionally a live progress UI

Every run of `ReindexParseBenchmark` always writes, on completion, both:

- the JSON report at the path given by `--out`;
- a static HTML report at the same path with `.html` in place of `.json` (e.g. `--out
  report.json` also writes `report.html` alongside it) - open it directly from disk in any
  browser, no server or network access needed, even after the tool's process has exited. It
  includes the run metadata, parameters, corpus summary, results table, a simple speedup-vs-
  thread-count bar chart (plain HTML/CSS bars, no charting library), and any failures/warnings,
  plus a "Print / Save as PDF" button and print-specific CSS for a clean printed/PDF result.

Both are written unconditionally - no flag is needed to get them, and no flag disables them.

Pass `--progress-ui` to additionally start a tiny local HTTP server (JDK-built-in
`com.sun.net.httpserver.HttpServer` only - no web framework dependency) on port 8090 for watching
a long sweep live: `/` shows a polling live-progress view (current thread-count sweep point,
repetition, WARCs completed/total, elapsed time), and once the sweep finishes `/report.html`
serves the exact same rendering as the static file (one shared rendering implementation,
`ReindexBenchReportHtml`, used by both), with `/report.json` serving the raw JSON. This is purely
an added convenience for watching a run in progress - the JSON/HTML files on disk are always the
authoritative record of a run, with or without `--progress-ui`. The server keeps running after
the sweep completes (so the final report page stays reachable); stop the process (e.g. Ctrl-C)
when done with it.
