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

import org.lockss.log.L4JLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Best-effort capture of the physical machine's hardware profile - CPU model, physical/logical
 * core counts (and, where determinable, a heterogeneous performance/efficiency core split),
 * total RAM, and storage/controller info - for inclusion in a {@link ReindexBenchReport}'s
 * {@link ReindexBenchReport.RunMetadata}.
 * <p>
 * This exists because a real dry run on a 10-"core" Apple Silicon Mac found reindex-parse
 * speedup plateauing at ~2.2x - which turned out to be fully explained by the machine actually
 * being 4 performance cores + 6 efficiency cores, not 10 homogeneous cores. Without this captured
 * in the report itself, a reader has to reconstruct that explanation by hand from `sysctl` output
 * every time. See au-reindex-recovery-plan.md's hardware-profile addendum.
 * <p>
 * <b>Design constraints, non-negotiable:</b> every probe here is read-only system inspection
 * (sysctl -n, /proc/*, system_profiler, lspci, lsblk - nothing that mutates state or needs
 * elevated privileges beyond the current user's). Every probe is wrapped so that ANY failure -
 * missing command, non-zero exit, malformed output, timeout, uncaught exception - degrades that
 * one field to "unknown"/"not determinable", never propagates, and never blocks or slows down
 * the actual benchmark sweep. Every shelled-out command has a short timeout and is destroyed on
 * expiry so a hung external command can never hang the benchmark run.
 */
public final class HardwareProfileDetector {

  private static final L4JLogger log = L4JLogger.getLogger();

  private static final long COMMAND_TIMEOUT_SECONDS = 5;

  private HardwareProfileDetector() {
  }

  /** Captures the hardware profile of the machine this JVM is running on. Never throws. */
  public static ReindexBenchReport.HardwareProfile capture() {
    ReindexBenchReport.HardwareProfile hw = new ReindexBenchReport.HardwareProfile();
    try {
      String osName = System.getProperty("os.name", "");
      if (isMac(osName)) {
        hw.platform = "macos";
        captureMac(hw);
      } else if (isLinux(osName)) {
        hw.platform = "linux";
        captureLinux(hw);
      } else {
        hw.platform = "unsupported";
        hw.notes = "Hardware profile capture is not supported on this platform (os.name=" + osName + ").";
      }
    } catch (Throwable t) {
      // Belt-and-suspenders: capture() itself must never throw, no matter what.
      log.debug("Unexpected failure capturing hardware profile; leaving fields unknown", t);
      hw.notes = "Hardware profile capture failed unexpectedly: " + t;
    }
    return hw;
  }

  private static boolean isMac(String osName) {
    return osName.toLowerCase(Locale.ROOT).contains("mac");
  }

  private static boolean isLinux(String osName) {
    return osName.toLowerCase(Locale.ROOT).contains("linux");
  }

  // ***********************************************************************************
  // * macOS
  // ***********************************************************************************

  private static void captureMac(ReindexBenchReport.HardwareProfile hw) {
    hw.cpuModel = runSysctl("machdep.cpu.brand_string");
    hw.physicalCores = parseIntOrNull(runSysctl("hw.physicalcpu"));
    hw.logicalCores = parseIntOrNull(runSysctl("hw.logicalcpu"));

    // Apple Silicon only - these keys simply don't exist on Intel Macs. That's not a failure,
    // it means "homogeneous" (or, more precisely, no P/E split to report).
    String perf0 = runSysctl("hw.perflevel0.physicalcpu");
    String perf1 = runSysctl("hw.perflevel1.physicalcpu");
    Integer perfCores = parseIntOrNull(perf0);
    Integer effCores = parseIntOrNull(perf1);
    if (perfCores != null && effCores != null) {
      hw.performanceCores = perfCores;
      hw.efficiencyCores = effCores;
      hw.coreTopology = "heterogeneous";
    } else {
      hw.coreTopology = "homogeneous or not applicable (no hw.perflevel* sysctl keys present - "
          + "expected on Intel Macs)";
    }

    String memBytes = runSysctl("hw.memsize");
    Long mem = parseLongOrNull(memBytes);
    hw.totalMemBytes = mem;

    captureMacStorage(hw);
  }

  private static void captureMacStorage(ReindexBenchReport.HardwareProfile hw) {
    // Prefer -json where available (modern macOS); fall back to plain-text regex extraction.
    String json = runCommand("system_profiler", "-json", "SPNVMeDataType");
    if (json != null && !json.isBlank()) {
      String model = extractJsonField(json, "device_model|_name");
      String medium = extractJsonField(json, "spnvme_physical_interconnect|spnvme_physical_interconnect_location");
      if (model != null) {
        hw.storageSummary = "NVMe: " + model;
        hw.storageDetectionMethod = "system_profiler -json SPNVMeDataType";
        return;
      }
    }

    String sataJson = runCommand("system_profiler", "-json", "SPSerialATADataType");
    if (sataJson != null && !sataJson.isBlank()) {
      String model = extractJsonField(sataJson, "_name");
      if (model != null) {
        hw.storageSummary = "SATA: " + model;
        hw.storageDetectionMethod = "system_profiler -json SPSerialATADataType";
        return;
      }
    }

    // Fall back to plain-text SPStorageDataType, which at minimum names the boot volume/device.
    String text = runCommand("system_profiler", "SPStorageDataType");
    if (text != null && !text.isBlank()) {
      String deviceName = extractPlainTextField(text, "Device Name:");
      String mediumType = extractPlainTextField(text, "Medium Type:");
      String protocol = extractPlainTextField(text, "Protocol:");
      StringBuilder sb = new StringBuilder();
      if (deviceName != null) {
        sb.append(deviceName);
      }
      if (mediumType != null) {
        sb.append(sb.length() > 0 ? " (" + mediumType + ")" : mediumType);
      }
      if (protocol != null) {
        sb.append(sb.length() > 0 ? ", " + protocol : protocol);
      }
      if (sb.length() > 0) {
        hw.storageSummary = sb.toString();
        hw.storageDetectionMethod = "system_profiler SPStorageDataType (plain text)";
        return;
      }
    }

    hw.storageSummary = "unknown (no storage info could be determined via system_profiler)";
    hw.storageDetectionMethod = "none succeeded";
  }

  /**
   * Extracts the first value for any of the given (pipe-separated) JSON field names from
   * semi-structured `system_profiler -json` output, without a full JSON parser dependency. Good
   * enough for this best-effort, single-value extraction; returns null on no match.
   */
  private static String extractJsonField(String json, String fieldNamesPipeSeparated) {
    for (String field : fieldNamesPipeSeparated.split("\\|")) {
      Pattern p = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"([^\"]*)\"");
      Matcher m = p.matcher(json);
      if (m.find()) {
        String v = m.group(1).trim();
        if (!v.isEmpty()) {
          return v;
        }
      }
    }
    return null;
  }

  private static String extractPlainTextField(String text, String label) {
    for (String line : text.split("\\R")) {
      String trimmed = line.trim();
      if (trimmed.startsWith(label)) {
        String v = trimmed.substring(label.length()).trim();
        if (!v.isEmpty()) {
          return v;
        }
      }
    }
    return null;
  }

  private static String runSysctl(String key) {
    return runCommand("sysctl", "-n", key);
  }

  // ***********************************************************************************
  // * Linux
  // ***********************************************************************************

  private static void captureLinux(ReindexBenchReport.HardwareProfile hw) {
    captureLinuxCpu(hw);
    captureLinuxMem(hw);
    captureLinuxStorage(hw);
  }

  private static void captureLinuxCpu(ReindexBenchReport.HardwareProfile hw) {
    Path cpuinfo = Paths.get("/proc/cpuinfo");
    if (!Files.isReadable(cpuinfo)) {
      hw.cpuModel = "unknown (/proc/cpuinfo not readable)";
      return;
    }
    try {
      String content = Files.readString(cpuinfo, StandardCharsets.UTF_8);
      String[] blocks = content.split("\\n\\n+");

      String model = null;
      Set<String> physicalCoreKeys = new HashSet<>();
      int logicalCount = 0;
      boolean sawPhysicalId = false;

      for (String block : blocks) {
        if (block.isBlank()) {
          continue;
        }
        boolean isProcessorEntry = false;
        String physicalId = null;
        String coreId = null;
        for (String line : block.split("\\R")) {
          int idx = line.indexOf(':');
          if (idx < 0) {
            continue;
          }
          String key = line.substring(0, idx).trim();
          String value = line.substring(idx + 1).trim();
          switch (key) {
            case "processor":
              isProcessorEntry = true;
              break;
            case "model name":
              if (model == null && !value.isEmpty()) {
                model = value;
              }
              break;
            case "physical id":
              physicalId = value;
              sawPhysicalId = true;
              break;
            case "core id":
              coreId = value;
              break;
            default:
              break;
          }
        }
        if (isProcessorEntry) {
          logicalCount++;
          if (physicalId != null && coreId != null) {
            physicalCoreKeys.add(physicalId + ":" + coreId);
          }
        }
      }

      hw.cpuModel = model != null ? model : "unknown (no 'model name' field in /proc/cpuinfo)";
      hw.logicalCores = logicalCount > 0 ? logicalCount : null;
      hw.physicalCores = sawPhysicalId && !physicalCoreKeys.isEmpty()
          ? physicalCoreKeys.size()
          // Some platforms (notably certain ARM/virtualized kernels) omit physical id/core id
          // entirely; in that case logical count is the best available approximation, but we
          // report it as unknown rather than silently assume no hyperthreading/SMT.
          : null;
    } catch (IOException | RuntimeException e) {
      log.debug("Failed to parse /proc/cpuinfo", e);
      hw.cpuModel = "unknown (failed to parse /proc/cpuinfo)";
    }

    // Hybrid Intel (12th-gen+) P/E-core detection: presence of these two sysfs paths together
    // indicates a heterogeneous core topology; absence means homogeneous (the overwhelming
    // common case, especially on Xeon/EPYC-class production servers).
    boolean hasCoreCpus = Files.exists(Paths.get("/sys/devices/cpu_core/cpus"));
    boolean hasAtomCpus = Files.exists(Paths.get("/sys/devices/cpu_atom/cpus"));
    if (hasCoreCpus && hasAtomCpus) {
      hw.coreTopology = "heterogeneous (hybrid Intel P/E cores detected via /sys/devices/cpu_core "
          + "and /sys/devices/cpu_atom, but per-tier counts were not further broken down)";
    } else {
      hw.coreTopology = "homogeneous or not determinable (no /sys/devices/cpu_core + cpu_atom "
          + "hybrid-core markers present)";
    }
  }

  private static void captureLinuxMem(ReindexBenchReport.HardwareProfile hw) {
    Path meminfo = Paths.get("/proc/meminfo");
    if (!Files.isReadable(meminfo)) {
      return;
    }
    try {
      for (String line : Files.readAllLines(meminfo, StandardCharsets.UTF_8)) {
        if (line.startsWith("MemTotal:")) {
          // Format: "MemTotal:       32821936 kB"
          Matcher m = Pattern.compile("(\\d+)\\s*kB").matcher(line);
          if (m.find()) {
            hw.totalMemBytes = Long.parseLong(m.group(1)) * 1024L;
          }
          return;
        }
      }
    } catch (IOException | RuntimeException e) {
      log.debug("Failed to parse /proc/meminfo", e);
    }
  }

  private static void captureLinuxStorage(ReindexBenchReport.HardwareProfile hw) {
    StringBuilder summary = new StringBuilder();

    String lspci = runCommand("lspci");
    if (lspci != null) {
      StringBuilder controllers = new StringBuilder();
      for (String line : lspci.split("\\R")) {
        if (line.toLowerCase(Locale.ROOT).matches(".*(raid|sas|sata|nvme|scsi).*")) {
          if (controllers.length() > 0) {
            controllers.append("; ");
          }
          controllers.append(line.trim());
        }
      }
      if (controllers.length() > 0) {
        summary.append("Controllers: ").append(controllers);
        hw.storageDetectionMethod = "lspci";
      }
    }

    String lsblk = runCommand("lsblk", "-d", "-o", "NAME,ROTA,MODEL");
    if (lsblk != null && !lsblk.isBlank()) {
      if (summary.length() > 0) {
        summary.append(" | ");
      }
      summary.append("Devices: ").append(lsblk.trim().replace("\n", "; "));
      hw.storageDetectionMethod = hw.storageDetectionMethod == null
          ? "lsblk"
          : hw.storageDetectionMethod + " + lsblk";
    }

    hw.storageSummary = summary.length() > 0
        ? summary.toString()
        : "unknown (lspci/lsblk unavailable or returned no matching info)";
    if (hw.storageDetectionMethod == null) {
      hw.storageDetectionMethod = "none succeeded";
    }
  }

  // ***********************************************************************************
  // * Shared command-running helper
  // ***********************************************************************************

  /**
   * Runs a read-only command with a short timeout, catching every possible failure mode and
   * returning null instead - never throws. Destroys the process on timeout so a hung command
   * (or an unexpectedly interactive one) can never hang the caller.
   */
  private static String runCommand(String... command) {
    Process proc = null;
    try {
      ProcessBuilder pb = new ProcessBuilder(command);
      // Redirect stderr to stdout's sibling /dev/null-equivalent (DISCARD) so a chatty command
      // writing to stderr can never fill an unread pipe and block the child - only stdout is
      // read, and only after waitFor confirms the process has actually exited.
      pb.redirectError(ProcessBuilder.Redirect.DISCARD);
      proc = pb.start();

      // Wait for exit (bounded by the timeout) BEFORE reading stdout, so a hung/interactive
      // command is destroyed on the timeout rather than potentially blocking forever on a
      // blocking read() against a child that never closes its stdout.
      boolean finished = proc.waitFor(COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!finished) {
        log.debug("Command {} timed out after {}s; destroying", String.join(" ", command),
            COMMAND_TIMEOUT_SECONDS);
        proc.destroyForcibly();
        return null;
      }
      if (proc.exitValue() != 0) {
        log.debug("Command {} exited {}", String.join(" ", command), proc.exitValue());
        return null;
      }

      // The process has already exited, so stdout is fully buffered/closed - this read cannot
      // block indefinitely.
      String output;
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
        StringBuilder sb = new StringBuilder();
        char[] buf = new char[8192];
        int n;
        while ((n = reader.read(buf)) != -1) {
          sb.append(buf, 0, n);
        }
        output = sb.toString();
      }

      String trimmed = output.trim();
      return trimmed.isEmpty() ? null : trimmed;
    } catch (Exception e) {
      log.debug("Command {} failed", String.join(" ", command), e);
      return null;
    } finally {
      if (proc != null && proc.isAlive()) {
        proc.destroyForcibly();
      }
    }
  }

  private static Integer parseIntOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Long parseLongOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Long.parseLong(s.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
