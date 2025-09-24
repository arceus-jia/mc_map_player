package me.example.mapframeplayer;

import com.google.gson.JsonArray;
import org.bukkit.Bukkit;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

class MediaManager {
    private final JavaPlugin plugin;

    private final Object mediaLock = new Object();
    private volatile boolean mediaInProgress = false;
    private volatile boolean mediaCancelRequested = false;
    private volatile String mediaCurrentName = null;
    private volatile Process currentTranscodeProc = null;

    private volatile Boolean ffmpegAvailableCache = null;
    private volatile long ffmpegLastCheckMs = 0L;

    MediaManager(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    List<String> listMediaEntries() {
        File framesRoot = new File(plugin.getDataFolder(), "frames");
        if (!framesRoot.exists() || !framesRoot.isDirectory())
            return Collections.emptyList();
        File[] dirs = framesRoot.listFiles();
        if (dirs == null)
            return Collections.emptyList();
        Arrays.sort(dirs, Comparator.comparing(File::getName));
        List<String> result = new ArrayList<>();
        for (File f : dirs) {
            if (f.isDirectory()) {
                File[] children = f.listFiles();
                int count = children == null ? 0 : children.length;
                result.add(f.getName() + " (" + count + " file" + (count == 1 ? "" : "s") + ")");
            } else {
                result.add(f.getName());
            }
        }
        return result;
    }

    void downloadMedia(String name, String url, CommandSender feedback) {
        String safeName = sanitizeName(name);
        if (safeName.isEmpty()) {
            sendFeedback(feedback, "Name is empty after sanitizing.");
            return;
        }
        if (!safeName.equals(name)) {
            sendFeedback(feedback, "Name adjusted to " + safeName + " for safety.");
        }
        synchronized (mediaLock) {
            if (mediaInProgress) {
                sendFeedback(feedback, "Another media task is running: '" + mediaCurrentName + "'. Try later or /mplay media cancel.");
                return;
            }
            mediaInProgress = true;
            mediaCancelRequested = false;
            mediaCurrentName = safeName;
        }

        File framesRoot = new File(plugin.getDataFolder(), "frames");
        if (!framesRoot.exists() && !framesRoot.mkdirs()) {
            sendFeedback(feedback, "Failed to create frames directory.");
            clearMediaState();
            return;
        }
        File targetDir = new File(framesRoot, safeName);
        if (!targetDir.exists() && !targetDir.mkdirs()) {
            sendFeedback(feedback, "Failed to create directory: " + targetDir.getAbsolutePath());
            clearMediaState();
            return;
        }
        if (hasExistingMedia(targetDir)) {
            sendFeedback(feedback,
                    "Media '" + safeName + "' already exists. Choose a different name or remove it first.");
            clearMediaState();
            return;
        }

        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                downloadToFolder(url, targetDir, feedback);
            } catch (IOException e) {
                if (!mediaCancelRequested)
                    sendFeedback(feedback, "Download failed: " + e.getMessage());
            } finally {
                clearMediaState();
            }
        });
    }

    void deleteMedia(String name, CommandSender feedback) {
        String safe = sanitizeName(name);
        if (safe.isEmpty()) {
            sendFeedback(feedback, "Invalid name.");
            return;
        }
        File framesRoot = new File(plugin.getDataFolder(), "frames");
        File target = new File(framesRoot, safe);
        if (!target.exists() || !target.isDirectory()) {
            sendFeedback(feedback, "Media '" + name + "' not found under frames/.");
            return;
        }
        try {
            deleteRecursively(target);
            sendFeedback(feedback, "Deleted media '" + safe + "'.");
        } catch (IOException e) {
            sendFeedback(feedback, "Delete failed: " + e.getMessage());
        }
    }

    void renameMedia(String oldName, String newName, CommandSender feedback) {
        String sOld = sanitizeName(oldName);
        String sNew = sanitizeName(newName);
        if (sOld.isEmpty() || sNew.isEmpty()) {
            sendFeedback(feedback, "Invalid name(s).");
            return;
        }
        File framesRoot = new File(plugin.getDataFolder(), "frames");
        File src = new File(framesRoot, sOld);
        File dst = new File(framesRoot, sNew);
        if (!src.exists() || !src.isDirectory()) {
            sendFeedback(feedback, "Source not found: '" + oldName + "'.");
            return;
        }
        if (dst.exists()) {
            sendFeedback(feedback, "Target already exists: '" + newName + "'.");
            return;
        }
        if (src.renameTo(dst)) {
            sendFeedback(feedback, "Renamed '" + sOld + "' -> '" + sNew + "'.");
            return;
        }
        try {
            copyRecursively(src, dst);
            deleteRecursively(src);
            sendFeedback(feedback, "Renamed '" + sOld + "' -> '" + sNew + "'.");
        } catch (IOException e) {
            sendFeedback(feedback, "Rename failed: " + e.getMessage());
        }
    }

    void transcodeMedia(String name, Integer fpsOpt, Integer targetHOpt, CommandSender feedback) {
        String safeName = sanitizeName(name);
        if (safeName.isEmpty()) {
            sendFeedback(feedback, "Invalid name.");
            return;
        }
        synchronized (mediaLock) {
            if (mediaInProgress) {
                sendFeedback(feedback, "Another media task is running: '" + mediaCurrentName + "'. Try later or /mplay media cancel.");
                return;
            }
            mediaInProgress = true;
            mediaCancelRequested = false;
            mediaCurrentName = safeName;
        }

        int fps = (fpsOpt != null && fpsOpt > 0) ? Math.min(fpsOpt, 60) : 20;
        int targetH = (targetHOpt != null && targetHOpt > 0) ? targetHOpt : 720;

        File framesRoot = new File(plugin.getDataFolder(), "frames");
        File dir = new File(framesRoot, safeName);
        if (!dir.exists() || !dir.isDirectory()) {
            sendFeedback(feedback, "Media '" + name + "' not found under frames/.");
            clearMediaState();
            return;
        }

        File src = pickSourceVideoFile(dir);
        if (src == null) {
            sendFeedback(feedback, "No video file found in '" + safeName + "'.");
            clearMediaState();
            return;
        }

        if (!isFfmpegAvailable()) {
            sendFeedback(feedback, "ffmpeg 不可用，无法转码。");
            clearMediaState();
            return;
        }

        final int height = targetH;
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                sendFeedback(feedback, "Transcoding '" + safeName + "' -> " + fps + "fps, <=" + height + "p ...");
                File outTmp = new File(dir, "transcoded.tmp.mp4");
                File outFinal = new File(dir, "default.mp4");

                String vf = String.format(Locale.US,
                        "scale=-2:%d:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2",
                        height);

                List<String> cmd = Arrays.asList(
                        "ffmpeg",
                        "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                        "-i", src.getAbsolutePath(),
                        "-vf", vf,
                        "-r", String.valueOf(fps),
                        "-an",
                        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                        "-pix_fmt", "yuv420p",
                        "-movflags", "+faststart",
                        outTmp.getAbsolutePath());

                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                currentTranscodeProc = p;
                try (InputStream pin = p.getInputStream()) {
                    byte[] buf = new byte[1024];
                    while (!mediaCancelRequested && pin.read(buf) != -1) {
                    }
                }
                if (mediaCancelRequested) {
                    try { p.destroyForcibly(); } catch (Throwable ignore) {}
                }
                int code = p.waitFor();
                currentTranscodeProc = null;

                if (mediaCancelRequested) {
                    try { outTmp.delete(); } catch (Throwable ignore) {}
                    sendFeedback(feedback, "转码已取消。");
                    return;
                }

                if (code == 0 && outTmp.exists() && outTmp.length() > 0) {
                    if (outFinal.exists()) {
                        if (!outFinal.delete()) {
                            sendFeedback(feedback, "转码成功但无法覆盖旧文件：" + outFinal.getName());
                            return;
                        }
                    }
                    if (!outTmp.renameTo(outFinal)) {
                        sendFeedback(feedback, "转码成功但重命名失败，文件保留为 " + outTmp.getName());
                    } else {
                        sendFeedback(feedback, "转码完成：" + outFinal.getName());
                    }
                } else {
                    sendFeedback(feedback, "转码失败 (exit=" + code + ")");
                    try { outTmp.delete(); } catch (Throwable ignore) {}
                }
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                if (!mediaCancelRequested)
                    sendFeedback(feedback, "转码失败：" + e.getMessage());
            } finally {
                clearMediaState();
            }
        });
    }

    void cancelCurrentMediaOperation(CommandSender feedback) {
        synchronized (mediaLock) {
            if (!mediaInProgress) {
                sendFeedback(feedback, "No active media task.");
                return;
            }
            mediaCancelRequested = true;
            Process p = currentTranscodeProc;
            if (p != null) {
                try { p.destroyForcibly(); } catch (Throwable ignore) {}
            }
        }
        sendFeedback(feedback, "Cancel requested for media task '" + mediaCurrentName + "'.");
    }

    private void clearMediaState() {
        synchronized (mediaLock) {
            mediaInProgress = false;
            mediaCancelRequested = false;
            mediaCurrentName = null;
            currentTranscodeProc = null;
        }
    }

    private File pickSourceVideoFile(File dir) {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) return null;
        for (File f : files) {
            String n = f.getName().toLowerCase(Locale.ROOT);
            if (n.startsWith("default") && isVideoExtension(extractExtension(n))) return f;
        }
        for (File f : files) {
            String ext = extractExtension(f.getName());
            if (ext != null && isVideoExtension(ext)) return f;
        }
        return null;
    }

    private void downloadToFolder(String url, File targetDir, CommandSender feedback) throws IOException {
        sendFeedback(feedback, "Starting download...");
        java.net.URL remote = new java.net.URL(url);
        java.net.URLConnection conn = remote.openConnection();
        conn.setRequestProperty("User-Agent", "MapFramePlayer/1.0");
        try { conn.setConnectTimeout(7000); conn.setReadTimeout(2000);} catch (Throwable ignore) {}
        String path = remote.getPath();
        String ext = determineExtension(path, conn.getContentType());
        File targetFile = new File(targetDir, "default" + ext);

        try (InputStream in = conn.getInputStream();
             FileOutputStream out = new FileOutputStream(targetFile)) {
            byte[] buf = new byte[8192];
            int read;
            long total = 0;
            while (!mediaCancelRequested && (read = safeRead(in, buf)) != -1) {
                if (read > 0) {
                    out.write(buf, 0, read);
                    total += read;
                }
            }
            if (mediaCancelRequested) {
                try { targetFile.delete(); } catch (Throwable ignore) {}
                sendFeedback(feedback, "Download canceled.");
                return;
            }
            sendFeedback(feedback, "Downloaded to " + targetFile.getName());
        }

        if (isVideoExtension(ext)) {
            if (!isFfmpegAvailable()) {
                sendFeedback(feedback, "ffmpeg 不可用，跳过 20fps 转码。");
                return;
            }
            try {
                sendFeedback(feedback, "Transcoding to 20fps (this may take a while)...");
                File outTmp = new File(targetDir, "default.transcode.mp4");
                List<String> cmd = Arrays.asList(
                        "ffmpeg",
                        "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                        "-i", targetFile.getAbsolutePath(),
                        "-r", "20",
                        "-an",
                        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                        "-pix_fmt", "yuv420p",
                        outTmp.getAbsolutePath());
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                currentTranscodeProc = p;
                try (InputStream pin = p.getInputStream()) { while (!mediaCancelRequested && pin.read() != -1) { } }
                if (mediaCancelRequested) { try { p.destroyForcibly(); } catch (Throwable ignore) {} }
                int code = p.waitFor();
                currentTranscodeProc = null;
                if (code == 0 && outTmp.exists() && outTmp.length() > 0) {
                    File outFinal = new File(targetDir, "default.mp4");
                    if (!targetFile.getName().equals(outFinal.getName())) {
                        try { targetFile.delete(); } catch (Throwable ignore) {}
                    }
                    if (outFinal.exists()) {
                        if (!outFinal.delete()) {
                            sendFeedback(feedback, "转码成功但无法覆盖旧文件：" + outFinal.getName());
                            return;
                        }
                    }
                    if (mediaCancelRequested) {
                        try { outTmp.delete(); } catch (Throwable ignore) {}
                        sendFeedback(feedback, "转码已取消，保留原视频：" + targetFile.getName());
                    } else if (!outTmp.renameTo(outFinal)) {
                        sendFeedback(feedback, "转码成功但重命名失败，文件保留为 " + outTmp.getName());
                    } else {
                        sendFeedback(feedback, "转码完成：" + outFinal.getName());
                    }
                } else {
                    sendFeedback(feedback, "转码失败 (exit=" + code + ")，保留原视频：" + targetFile.getName());
                    try { outTmp.delete(); } catch (Throwable ignore) {}
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                sendFeedback(feedback, "转码被中断，保留原视频：" + targetFile.getName());
            } catch (IOException e) {
                sendFeedback(feedback, "转码失败：" + e.getMessage());
            }
        }
    }

    private boolean isFfmpegAvailable() {
        long now = System.currentTimeMillis();
        if (ffmpegAvailableCache != null && (now - ffmpegLastCheckMs) < 30_000L)
            return ffmpegAvailableCache;
        boolean ok = false;
        try {
            ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-version");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            try {
                boolean done = p.waitFor(1500, java.util.concurrent.TimeUnit.MILLISECONDS);
                ok = done ? (p.exitValue() == 0) : true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                ok = true; // assume present on interruption
            } finally {
                try { p.destroyForcibly(); } catch (Throwable ignore) {}
            }
        } catch (IOException | RuntimeException e) {
            ok = false;
        }
        ffmpegAvailableCache = ok;
        ffmpegLastCheckMs = now;
        return ok;
    }

    private String sanitizeName(String raw) {
        return raw == null ? "" : raw.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private void sendFeedback(CommandSender receiver, String message) {
        if (receiver == null) return;
        Bukkit.getScheduler().runTask(plugin, () -> receiver.sendMessage("[mplay] " + message));
    }

    private boolean hasExistingMedia(File dir) {
        File[] files = dir.listFiles();
        return files != null && files.length > 0;
    }

    private void deleteRecursively(File f) throws IOException {
        if (f.isDirectory()) {
            File[] kids = f.listFiles();
            if (kids != null) for (File k : kids) deleteRecursively(k);
        }
        if (!f.delete()) throw new IOException("failed to delete: " + f.getAbsolutePath());
    }

    private void copyRecursively(File src, File dst) throws IOException {
        if (src.isDirectory()) {
            if (!dst.exists() && !dst.mkdirs()) throw new IOException("failed to mkdirs: " + dst.getAbsolutePath());
            File[] kids = src.listFiles();
            if (kids != null) {
                for (File k : kids) copyRecursively(k, new File(dst, k.getName()));
            }
        } else {
            Files.copy(src.toPath(), dst.toPath());
        }
    }

    private String determineExtension(String path, String contentType) {
        String ext = extractExtension(path);
        if (ext != null) return ext;
        if (contentType != null) {
            String ct = contentType.toLowerCase(Locale.ROOT);
            if (ct.contains("json")) return ".json";
            if (ct.contains("smrf")) return ".smrf";
            if (ct.contains("png")) return ".png";
            if (ct.contains("jpeg") || ct.contains("jpg")) return ".jpg";
        }
        return ".mp4";
    }

    private String extractExtension(String path) {
        if (path == null) return null;
        int dot = path.lastIndexOf('.');
        if (dot <= 0 || dot == path.length() - 1) return null;
        String candidate = path.substring(dot).toLowerCase(Locale.ROOT);
        if (candidate.length() > 5) return null;
        if (candidate.matches("\\.[a-z0-9]{1,4}")) return candidate;
        return null;
    }

    private boolean isVideoExtension(String extWithDot) {
        if (extWithDot == null) return false;
        String e = extWithDot.toLowerCase(Locale.ROOT);
        return e.equals(".mp4") || e.equals(".mov") || e.equals(".m4v") ||
                e.equals(".avi") || e.equals(".webm") || e.equals(".wmv") ||
                e.equals(".ts") || e.equals(".gif") || e.equals(".mkv") || e.equals(".m3u8");
    }

    private int safeRead(InputStream in, byte[] buf) throws IOException {
        try {
            return in.read(buf);
        } catch (java.net.SocketTimeoutException ste) {
            return 0;
        }
    }
}
