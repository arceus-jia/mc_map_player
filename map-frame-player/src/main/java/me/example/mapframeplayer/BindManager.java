package me.example.mapframeplayer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.entity.Entity;
import org.bukkit.entity.ItemFrame;
import org.bukkit.entity.Player;
import org.bukkit.command.CommandSender;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.MapMeta;
import org.bukkit.map.MapPalette;
import org.bukkit.map.MapRenderer;
import org.bukkit.map.MapView;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.imageio.ImageIO;

class BindManager {
    private volatile boolean DEBUG;

    private final JavaPlugin plugin;
    private final PlayerManager playerManager;
    private final FrameSourceLoader frameSourceLoader;

    private final Map<Integer, ScreenSession> sessions = new LinkedHashMap<>();
    private int nextScreenId = 1;
    private Integer lastActiveId = null;

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final Type persistedListType = new TypeToken<List<PersistedScreen>>() {
    }.getType();

    private byte[] lut = null;

    private int tickerTask = -1;

    // cache for ffmpeg detection
    private volatile Boolean ffmpegAvailableCache = null;
    private volatile long ffmpegLastCheckMs = 0L;

    // media download/transcode single-flight control
    private final Object mediaLock = new Object();
    private volatile boolean mediaInProgress = false;
    private volatile boolean mediaCancelRequested = false;
    private volatile String mediaCurrentName = null;
    private volatile Process currentTranscodeProc = null;

    BindManager(JavaPlugin plugin, PlayerManager playerManager) {
        this.plugin = plugin;
        this.playerManager = playerManager;
        this.frameSourceLoader = new FrameSourceLoader(plugin);
        this.DEBUG = plugin.getConfig().getBoolean("debug", false);
    }

    void loadPersistedScreens() {
        File file = persistenceFile();
        if (!file.exists())
            return;

        try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
            List<PersistedScreen> data = gson.fromJson(reader, persistedListType);
            if (data == null)
                return;

            boolean dirty = false;
            for (PersistedScreen screen : data) {
                if (!restorePersistedScreen(screen))
                    dirty = true;
            }

            if (dirty)
                saveSessions();
        } catch (IOException | RuntimeException e) {
            plugin.getLogger().warning("[mplay] Failed to load screens.json: " + e.getMessage());
        }
    }

    void setLUT(byte[] lut) {
        this.lut = lut;
    }

    boolean isLutLoaded() {
        return this.lut != null;
    }

    void setDebug(boolean debug) {
        this.DEBUG = debug;
    }

    boolean willUseFfmpeg(int screenId) {
        ScreenSession s = sessions.get(screenId);
        return s != null && s.videoMode;
    }

    boolean isFfmpegAvailable() {
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
                ok = done ? (p.exitValue() == 0) : true; // if slow, assume present
            } finally {
                try { p.destroyForcibly(); } catch (Throwable ignore) {}
            }
        } catch (IOException e) {
            ok = false;
        } catch (Throwable t) {
            ok = false;
        }
        ffmpegAvailableCache = ok;
        ffmpegLastCheckMs = now;
        return ok;
    }

    void ensureTicker() {
        if (tickerTask != -1)
            return;

        tickerTask = Bukkit.getScheduler().scheduleSyncRepeatingTask(plugin, () -> {
            MapFramePlayer.TICK++;
            for (ScreenSession session : new ArrayList<>(sessions.values())) {
                session.onTick(MapFramePlayer.TICK);
            }
        }, 1L, 1L);
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

    boolean isMediaInProgress() { return mediaInProgress; }

    private void clearMediaState() {
        synchronized (mediaLock) {
            mediaInProgress = false;
            mediaCancelRequested = false;
            mediaCurrentName = null;
            currentTranscodeProc = null;
        }
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

        // pick a source video file
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

                // 更稳健的缩放：
                // 1) scale 到不超过目标高度（不放大），保持比例：force_original_aspect_ratio=decrease
                // 2) 再次 scale 到偶数尺寸，避免编码器报错
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
                        // drain
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

    private File pickSourceVideoFile(File dir) {
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) return null;
        // 优先 default.*
        for (File f : files) {
            String n = f.getName().toLowerCase(Locale.ROOT);
            if (n.startsWith("default") && isVideoExtension(extractExtension(n))) return f;
        }
        // 其次挑第一个视频文件
        for (File f : files) {
            String ext = extractExtension(f.getName());
            if (ext != null && isVideoExtension(ext)) return f;
        }
        return null;
    }

    void startLiveStream(int screenId, String source, int requestedTpf, Integer bufferFrames,
            CommandSender feedback) {
        ScreenSession session = sessions.get(screenId);
        if (session == null) {
            sendFeedback(feedback, "Screen #" + screenId + " not found.");
            return;
        }
        if (session.group == null || session.group.members == null || session.group.members.isEmpty()) {
            sendFeedback(feedback, "Screen #" + screenId + " has no binding. Create or set first.");
            return;
        }

        String url = null;
        String label = source;
        if (source.contains("://")) {
            url = source;
        } else {
            String safeName = sanitizeName(source);
            File framesRoot = new File(plugin.getDataFolder(), "frames");
            File targetDir = new File(framesRoot, safeName);
            if (!targetDir.exists() || !targetDir.isDirectory()) {
                sendFeedback(feedback, "Media '" + source + "' not found under frames/.");
                return;
            }
            File candidate = new File(targetDir, "bilibili.m3u8.txt");
            if (!candidate.exists()) {
                sendFeedback(feedback, "Media '" + source + "' does not contain bilibili.m3u8.txt.");
                return;
            }
            try {
                url = Files.readString(candidate.toPath(), StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                sendFeedback(feedback, "Failed to read " + candidate.getName() + ": " + e.getMessage());
                return;
            }
            if (url.isEmpty()) {
                sendFeedback(feedback, "bilibili.m3u8.txt is empty.");
                return;
            }
            label = source + "/bilibili";
        }

        session.startLiveStream(url, label, requestedTpf, bufferFrames != null ? bufferFrames : 0, feedback);
        saveSessions();
        sendFeedback(feedback, "Screen #" + screenId + " streaming from " + url);
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

    private void downloadToFolder(String url, File targetDir, CommandSender feedback) throws IOException {
        sendFeedback(feedback, "Starting download...");
        java.net.URL remote = new java.net.URL(url);
        java.net.URLConnection conn = remote.openConnection();
        conn.setRequestProperty("User-Agent", "MapFramePlayer/1.0");
        try {
            conn.setConnectTimeout(7000);
            conn.setReadTimeout(2000);
        } catch (Throwable ignore) {}
        String path = remote.getPath();
        String ext = determineExtension(path, conn.getContentType());
        File targetFile = new File(targetDir, "default" + ext);

        try (InputStream in = conn.getInputStream();
                java.io.FileOutputStream out = new java.io.FileOutputStream(targetFile)) {
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
        // 如果下载的是视频，尝试转码为 20fps 并统一为 mp4
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
                try (InputStream pin = p.getInputStream()) { while (!mediaCancelRequested && pin.read() != -1) { /* drain */ } }
                if (mediaCancelRequested) {
                    try { p.destroyForcibly(); } catch (Throwable ignore) {}
                }
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

    private int safeRead(InputStream in, byte[] buf) throws IOException {
        try {
            return in.read(buf);
        } catch (java.net.SocketTimeoutException ste) {
            return 0; // treat as no data, allow cancel check
        }
    }

    private String determineExtension(String path, String contentType) {
        String ext = extractExtension(path);
        if (ext != null)
            return ext;
        if (contentType != null) {
            if (contentType.contains("json"))
                return ".json";
            if (contentType.contains("smrf"))
                return ".smrf";
            if (contentType.contains("png"))
                return ".png";
            if (contentType.contains("jpeg") || contentType.contains("jpg"))
                return ".jpg";
        }
        return ".mp4";
    }

    private boolean isVideoExtension(String extWithDot) {
        if (extWithDot == null) return false;
        String e = extWithDot.toLowerCase(Locale.ROOT);
        return e.equals(".mp4") || e.equals(".mov") || e.equals(".m4v") ||
                e.equals(".avi") || e.equals(".webm") || e.equals(".wmv") ||
                e.equals(".ts") || e.equals(".gif") || e.equals(".mkv") || e.equals(".m3u8");
    }

    private String extractExtension(String path) {
        int dot = path.lastIndexOf('.');
        if (dot <= 0 || dot == path.length() - 1)
            return null;
        String candidate = path.substring(dot).toLowerCase(Locale.ROOT);
        if (candidate.length() > 5)
            return null;
        if (candidate.matches("\\.[a-z0-9]{1,4}"))
            return candidate;
        return null;
    }

    private String humanReadableBytes(long bytes) {
        if (bytes < 1024)
            return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = ("KMGTPE").charAt(exp - 1) + "";
        return String.format(Locale.US, "%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    private String sanitizeName(String raw) {
        return raw == null ? "" : raw.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    void fetchBilibiliStream(String name, String roomId, CommandSender feedback) {
        String safeName = sanitizeName(name);
        if (safeName.isEmpty()) {
            sendFeedback(feedback, "Name is empty after sanitizing.");
            return;
        }
        if (!safeName.equals(name))
            sendFeedback(feedback, "Name adjusted to " + safeName + " for safety.");

        File framesRoot = new File(plugin.getDataFolder(), "frames");
        if (!framesRoot.exists() && !framesRoot.mkdirs()) {
            sendFeedback(feedback, "Failed to create frames directory.");
            return;
        }
        File targetDir = new File(framesRoot, safeName);
        if (!targetDir.exists() && !targetDir.mkdirs()) {
            sendFeedback(feedback, "Failed to create directory: " + targetDir.getAbsolutePath());
            return;
        }
        if (hasExistingMedia(targetDir)) {
            sendFeedback(feedback,
                    "Media '" + safeName + "' already exists. Choose a different name or remove it first.");
            return;
        }

        sendFeedback(feedback, "Resolving bilibili live for room " + roomId + "...");
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                String streamUrl = resolveBilibiliM3U8(roomId);
                File targetFile = new File(targetDir, "bilibili.m3u8.txt");
                try (FileOutputStream out = new FileOutputStream(targetFile)) {
                    out.write(streamUrl.getBytes(StandardCharsets.UTF_8));
                    out.write('\n');
                }
                sendFeedback(feedback, "Saved bilibili stream URL to " + targetFile.getName());
            } catch (IOException e) {
                sendFeedback(feedback, "Bilibili resolve failed: " + e.getMessage());
            }
        });
    }

    private boolean hasExistingMedia(File dir) {
        File[] files = dir.listFiles();
        return files != null && files.length > 0;
    }

    private void sendFeedback(CommandSender receiver, String message) {
        if (receiver == null)
            return;
        Bukkit.getScheduler().runTask(plugin, () -> receiver.sendMessage("[mplay] " + message));
    }

    private void deleteRecursively(File f) throws IOException {
        if (f.isDirectory()) {
            File[] kids = f.listFiles();
            if (kids != null) {
                for (File k : kids) deleteRecursively(k);
            }
        }
        if (!f.delete()) throw new IOException("failed to delete: " + f.getAbsolutePath());
    }

    private void copyRecursively(File src, File dst) throws IOException {
        if (src.isDirectory()) {
            if (!dst.exists() && !dst.mkdirs()) throw new IOException("failed to mkdirs: " + dst.getAbsolutePath());
            File[] kids = src.listFiles();
            if (kids != null) for (File k : kids) copyRecursively(k, new File(dst, k.getName()));
        } else {
            java.nio.file.Files.copy(src.toPath(), dst.toPath());
        }
    }

    private String resolveBilibiliM3U8(String roomId) throws IOException {
        String api = "https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id=" + roomId
                + "&protocol=0,1&format=0,1,2&codec=0,1&qn=0&platform=web&ptype=8";
        HttpURLConnection conn = (HttpURLConnection) new URL(api).openConnection();
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(7000);
        conn.setRequestProperty("User-Agent", "Mozilla/5.0 (MapFramePlayer)");
        conn.setRequestProperty("Referer", "https://live.bilibili.com/" + roomId);

        int http = conn.getResponseCode();
        if (http != HttpURLConnection.HTTP_OK)
            throw new IOException("HTTP " + http + " while requesting bilibili API");

        String body;
        try (InputStream in = conn.getInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) != -1)
                out.write(buf, 0, len);
            body = out.toString(StandardCharsets.UTF_8.name());
        }

        JsonObject root = JsonParser.parseString(body).getAsJsonObject();
        if (!root.has("code") || root.get("code").getAsInt() != 0)
            throw new IOException("API response error: " + root.get("code"));
        JsonObject data = root.getAsJsonObject("data");
        if (data == null)
            throw new IOException("Missing data field");
        JsonObject playurlInfo = data.getAsJsonObject("playurl_info");
        if (playurlInfo == null)
            throw new IOException("Missing playurl_info field");
        JsonObject playurl = playurlInfo.getAsJsonObject("playurl");
        if (playurl == null)
            throw new IOException("Missing playurl field");

        JsonArray streams = playurl.getAsJsonArray("stream");
        if (streams == null || streams.size() == 0)
            throw new IOException("No available stream data");

        for (JsonElement streamElem : streams) {
            JsonObject stream = streamElem.getAsJsonObject();
            JsonArray formats = stream.getAsJsonArray("format");
            if (formats == null)
                continue;
            for (JsonElement formatElem : formats) {
                JsonObject format = formatElem.getAsJsonObject();
                JsonArray codecs = format.getAsJsonArray("codec");
                if (codecs == null)
                    continue;
                for (JsonElement codecElem : codecs) {
                    JsonObject codec = codecElem.getAsJsonObject();
                    String baseUrl = codec.has("base_url") ? codec.get("base_url").getAsString() : null;
                    JsonArray urlInfos = codec.getAsJsonArray("url_info");
                    if (baseUrl == null || urlInfos == null || urlInfos.size() == 0)
                        continue;
                    JsonObject info = urlInfos.get(0).getAsJsonObject();
                    String host = info.get("host").getAsString();
                    String extra = info.has("extra") ? info.get("extra").getAsString() : "";
                    if (host != null)
                        return host + baseUrl + extra;
                }
            }
        }

        throw new IOException("Unable to locate a playable URL");
    }

    void stop() {
        for (ScreenSession session : new ArrayList<>(sessions.values())) {
            session.stopPlayback();
            session.stopPreloader();
        }
        saveSessions();
        sessions.clear();
        lastActiveId = null;
        if (tickerTask != -1) {
            Bukkit.getScheduler().cancelTask(tickerTask);
            tickerTask = -1;
        }
    }

    Integer createAndBindFloatingAtPlayer(Player p, int cols, int rows, int radius, int forward, Location anchor) {
        World w = p.getWorld();
        BlockFace face = p.getFacing();
        BlockFace right = rightOf(face);

        Block base = (anchor != null && anchor.getWorld() == w)
                ? w.getBlockAt(anchor.getBlockX(), anchor.getBlockY(), anchor.getBlockZ())
                : p.getLocation().getBlock().getRelative(face, Math.max(1, forward));

        Block backingBase = base.getRelative(face);

        List<Block> placedBarriers = new ArrayList<>();
        for (int r = 0; r < rows; r++) {
            int yOff = (rows - 1 - r);
            for (int c = 0; c < cols; c++) {
                Block backing = backingBase.getRelative(right, c).getRelative(BlockFace.UP, yOff);
                Material t = backing.getType();
                if (t == Material.AIR || t == Material.CAVE_AIR || t == Material.VOID_AIR) {
                    backing.setType(Material.BARRIER, false);
                    placedBarriers.add(backing);
                }
            }
        }

        int[] ids = new int[cols * rows];
        int idx = 0;
        List<ItemFrame> frames = new ArrayList<>();
        for (int r = 0; r < rows; r++) {
            int yOff = (rows - 1 - r);
            for (int c = 0; c < cols; c++) {
                Block backing = base.getRelative(right, c).getRelative(BlockFace.UP, yOff);
                Location floc = backing.getLocation().add(0.5, 0.5, 0.5);

                MapView view = Bukkit.createMap(w);
                ItemStack mapItem = new ItemStack(Material.FILLED_MAP, 1);
                MapMeta meta = (MapMeta) mapItem.getItemMeta();
                if (meta != null) {
                    meta.setMapView(view);
                    mapItem.setItemMeta(meta);
                }

                ItemFrame frame = w.spawn(floc, ItemFrame.class, f -> {
                    BlockFace attachFace = face.getOppositeFace();
                    f.setFacingDirection(attachFace, true);
                    f.setItem(mapItem, false);
                    f.setVisible(true);
                    try {
                        f.setFixed(true);
                    } catch (Throwable ignore) {
                    }
                });
                frames.add(frame);

                try {
                    ids[idx++] = view.getId();
                } catch (Throwable t) {
                    cleanupFrames(frames);
                    cleanupBarriers(placedBarriers);
                    plugin.getLogger().warning("[mplay] failed to get map id: " + t.getMessage());
                    return null;
                }
            }
        }

        int rx = right.getModX(), rz = right.getModZ();
        double cx = base.getX() + 0.5 + rx * ((cols - 1) / 2.0);
        double cy = base.getY() + 0.5 + ((rows - 1) / 2.0);
        double cz = base.getZ() + 0.5 + rz * ((cols - 1) / 2.0);
        Location center = new Location(w, cx, cy, cz);

        Integer screenId = bind(ids, w.getName(), cols, rows, radius, center);
        if (screenId == null) {
            cleanupFrames(frames);
            cleanupBarriers(placedBarriers);
            return null;
        }

        ScreenSession session = sessions.get(screenId);
        if (session != null) {
            session.recordPlacedBarriers(placedBarriers);
            session.recordFrames(frames);
            session.initBlackFrame();
            saveSessions();
        }
        return screenId;
    }

    Integer bind(int[] mapIds, String worldName, int cols, int rows, int radius, Location center) {
        World w = Bukkit.getWorld(worldName);
        if (w == null)
            return null;

        Location centerLoc = (center != null && center.getWorld() == w) ? center.clone() : w.getSpawnLocation();

        BindingGroup group = new BindingGroup(worldName, cols, rows, radius);
        group.center = centerLoc;
        group.members = new ArrayList<>(mapIds.length);

        int idx = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                int mapId = mapIds[idx++];
                MapView view = Bukkit.getMap(mapId);
                if (view == null)
                    return null;

                for (MapRenderer r0 : new ArrayList<>(view.getRenderers()))
                    view.removeRenderer(r0);
                DataRenderer renderer = new DataRenderer();
                view.setScale(MapView.Scale.NORMAL);
                view.setTrackingPosition(false);
                view.setUnlimitedTracking(false);
                view.addRenderer(renderer);

                renderer.setRange(w, centerLoc, radius);

                group.members.add(new Binding(mapId, w, view, renderer));
            }
        }

        ScreenSession session = registerSession(group);
        dbg("registered screen id=" + session.id + " layout=" + cols + "x" + rows + " radius=" + radius);
        return session.id;
    }

    int loadFramesFromFolder(int screenId, String folderPath) {
        ScreenSession session = sessions.get(screenId);
        if (session == null)
            return -1;
        lastActiveId = screenId;
        return session.loadFramesFromFolder(folderPath);
    }

    void startPlayback(int screenId, int tpf, boolean loop, int bufferTarget, int warmupTicks) {
        ScreenSession session = sessions.get(screenId);
        if (session == null)
            return;
        lastActiveId = screenId;
        session.startPlayback(tpf, loop, bufferTarget, warmupTicks);
    }

    boolean stopPlayback(int screenId) {
        ScreenSession session = sessions.get(screenId);
        if (session == null)
            return false;
        session.stopPlayback();
        session.stopPreloader();
        lastActiveId = screenId;
        return true;
    }

    int stopAllPlayback() {
        int count = 0;
        for (ScreenSession session : sessions.values()) {
            session.stopPlayback();
            session.stopPreloader();
            count++;
        }
        return count;
    }

    boolean clearScreen(int screenId) {
        ScreenSession session = sessions.get(screenId);
        if (session == null)
            return false;
        session.clearScreen();
        sessions.remove(screenId);
        if (screenId == lastActiveId) {
            lastActiveId = sessions.isEmpty() ? null : sessions.keySet().iterator().next();
        }
        saveSessions();
        return true;
    }

    int clearAllScreens() {
        int cleared = 0;
        for (Integer id : new ArrayList<>(sessions.keySet())) {
            if (clearScreen(id))
                cleared++;
        }
        if (cleared == 0)
            saveSessions();
        return cleared;
    }

    boolean resetToBlack(int screenId) {
        ScreenSession session = sessions.get(screenId);
        if (session == null)
            return false;
        session.resetToBlack();
        lastActiveId = screenId;
        return true;
    }

    String statusString(int screenId) {
        ScreenSession session = sessions.get(screenId);
        if (session == null)
            return "no such screen";
        return session.describe();
    }

    List<String> listStatus() {
        List<String> out = new ArrayList<>();
        for (ScreenSession session : sessions.values()) {
            out.add(session.describeBrief());
        }
        return out;
    }

    Integer lastActiveId() {
        return lastActiveId;
    }

    boolean hasScreens() {
        return !sessions.isEmpty();
    }

    private ScreenSession registerSession(BindingGroup group) {
        return registerSessionInternal(group, null, true);
    }

    private void cleanupFrames(List<ItemFrame> frames) {
        for (ItemFrame fr : frames) {
            try {
                fr.remove();
            } catch (Throwable ignore) {
            }
        }
    }

    private void cleanupBarriers(List<Block> barriers) {
        for (Block b : barriers) {
            try {
                b.setType(Material.AIR, false);
            } catch (Throwable ignore) {
            }
        }
    }

    private static BlockFace rightOf(BlockFace face) {
        switch (face) {
            case NORTH:
                return BlockFace.EAST;
            case SOUTH:
                return BlockFace.WEST;
            case EAST:
                return BlockFace.SOUTH;
            case WEST:
                return BlockFace.NORTH;
            default:
                return BlockFace.EAST;
        }
    }

    private void dbg(String msg) {
        if (DEBUG)
            plugin.getLogger().info("[mplay] " + msg);
    }

    private File persistenceFile() {
        return new File(plugin.getDataFolder(), "screens.json");
    }

    private void saveSessions() {
        try {
            if (!plugin.getDataFolder().exists() && !plugin.getDataFolder().mkdirs()) {
                plugin.getLogger().warning("[mplay] Failed to create data folder for persistence.");
                return;
            }
            List<PersistedScreen> data = new ArrayList<>();
            for (ScreenSession session : sessions.values()) {
                data.add(toPersistedScreen(session));
            }
            String json = gson.toJson(data, persistedListType);
            Files.writeString(persistenceFile().toPath(), json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            plugin.getLogger().warning("[mplay] Failed to save screens.json: " + e.getMessage());
        }
    }

    private PersistedScreen toPersistedScreen(ScreenSession session) {
        BindingGroup g = session.group;
        PersistedScreen ps = new PersistedScreen();
        ps.id = session.id;
        ps.world = g.worldName;
        ps.cols = g.cols;
        ps.rows = g.rows;
        ps.radius = g.radius;
        if (g.center != null) {
            ps.centerX = g.center.getX();
            ps.centerY = g.center.getY();
            ps.centerZ = g.center.getZ();
        }
        ps.mapIds = new ArrayList<>(g.members.size());
        for (Binding b : g.members)
            ps.mapIds.add(b.mapId);

        ps.barriers = new ArrayList<>(g.placedBarriers.size());
        for (Location loc : g.placedBarriers) {
            BlockPos bp = new BlockPos();
            bp.x = loc.getBlockX();
            bp.y = loc.getBlockY();
            bp.z = loc.getBlockZ();
            ps.barriers.add(bp);
        }

        ps.frames = new ArrayList<>(g.frameLocations.size());
        for (Location loc : g.frameLocations) {
            Vec3 v = new Vec3();
            v.x = loc.getX();
            v.y = loc.getY();
            v.z = loc.getZ();
            ps.frames.add(v);
        }
        ps.sourceLabel = session.sourceLabel;
        ps.video = session.videoMode;
        return ps;
    }

    private boolean restorePersistedScreen(PersistedScreen ps) {
        if (ps.world == null || ps.mapIds == null || ps.mapIds.isEmpty()) {
            plugin.getLogger().warning("[mplay] Skip persisted screen with missing data: " + ps.id);
            return false;
        }
        World w = Bukkit.getWorld(ps.world);
        if (w == null) {
            plugin.getLogger().warning("[mplay] World not found for screen #" + ps.id + ": " + ps.world);
            return false;
        }
        if (ps.cols <= 0 || ps.rows <= 0 || ps.mapIds.size() != ps.cols * ps.rows) {
            plugin.getLogger().warning("[mplay] Screen #" + ps.id + " size/map mismatch.");
            return false;
        }

        BindingGroup group = new BindingGroup(ps.world, ps.cols, ps.rows, ps.radius);
        Location center = (ps.centerX != null && ps.centerY != null && ps.centerZ != null)
                ? new Location(w, ps.centerX, ps.centerY, ps.centerZ)
                : null;
        group.center = center;
        group.members = new ArrayList<>(ps.mapIds.size());

        for (int mapId : ps.mapIds) {
            MapView view = Bukkit.getMap(mapId);
            if (view == null) {
                plugin.getLogger().warning("[mplay] Map " + mapId + " missing for screen #" + ps.id + ".");
                return false;
            }
            for (MapRenderer r0 : new ArrayList<>(view.getRenderers()))
                view.removeRenderer(r0);
            DataRenderer renderer = new DataRenderer();
            view.setScale(MapView.Scale.NORMAL);
            view.setTrackingPosition(false);
            view.setUnlimitedTracking(false);
            view.addRenderer(renderer);

            Location centerForRenderer = (center != null) ? center : w.getSpawnLocation();
            renderer.setRange(w, centerForRenderer, ps.radius);

            group.members.add(new Binding(mapId, w, view, renderer));
        }

        group.placedBarriers.clear();
        if (ps.barriers != null) {
            for (BlockPos bp : ps.barriers) {
                group.placedBarriers.add(new Location(w, bp.x, bp.y, bp.z));
            }
        }

        group.frameLocations.clear();
        if (ps.frames != null) {
            for (Vec3 v : ps.frames) {
                group.frameLocations.add(new Location(w, v.x, v.y, v.z));
            }
        }

        ScreenSession session = registerSessionInternal(group, ps.id, false);
        session.sourceLabel = ps.sourceLabel != null ? ps.sourceLabel : "";
        session.initBlackFrame();
        return true;
    }

    private ScreenSession registerSessionInternal(BindingGroup group, Integer forcedId, boolean persist) {
        int id = (forcedId != null) ? forcedId : nextScreenId++;
        if (forcedId != null)
            nextScreenId = Math.max(nextScreenId, forcedId + 1);
        ScreenSession session = new ScreenSession(id, group);
        sessions.put(id, session);
        lastActiveId = id;
        if (persist)
            saveSessions();
        return session;
    }

    private static class PersistedScreen {
        int id;
        String world;
        int cols;
        int rows;
        int radius;
        Double centerX;
        Double centerY;
        Double centerZ;
        List<Integer> mapIds;
        List<BlockPos> barriers;
        List<Vec3> frames;
        String sourceLabel;
        boolean video;
    }

    private static class BlockPos {
        int x;
        int y;
        int z;
    }

    private static class Vec3 {
        double x;
        double y;
        double z;
    }

    private class ScreenSession {
        final int id;
        final BindingGroup group;
        final int maxDistance;
        private static final int DEFAULT_LIVE_QUEUE_LIMIT = 30;
        private String sourceLabel = "";

        private final ConcurrentLinkedQueue<byte[]> buffer = new ConcurrentLinkedQueue<>();
        private volatile boolean preloadRunning = false;
        private BukkitTask preloadTask = null;
        private BukkitTask ffmpegTask = null;
        private Process ffmpegProc = null;

        private boolean videoMode = false;
        private File videoFile = null;
        private boolean liveMode = false;
        private String liveUrl = null;
        private int liveQueueLimit = DEFAULT_LIVE_QUEUE_LIMIT;
        private boolean dumpedFirstFrame = false;
        private boolean frameSizeWarningLogged = false;
        private CommandSender liveStarter = null;
        private BukkitTask progressTask = null;
        private boolean firstFrameAnnounced = false;
        private long liveStartNano = 0L;
        private int liveConnectAttempts = 0;

        private List<File> frames = Collections.emptyList();
        private int frameIndex = 0;
        private int ticksPerFrame = 2;
        private boolean loop = false;

        private int bufferTarget = 0;
        private long startTick = 0L;
        private long nextFrameTick = Long.MAX_VALUE;
        private int warmupTicks = 0;

        private ScreenSession(int id, BindingGroup group) {
            this.id = id;
            this.group = group;
            this.maxDistance = group.radius;
        }

        void recordPlacedBarriers(List<Block> placed) {
            group.placedBarriers.clear();
            for (Block b : placed)
                group.placedBarriers.add(b.getLocation());
        }

        void recordFrames(List<ItemFrame> frames) {
            group.frameUUIDs.clear();
            group.frameLocations.clear();
            for (ItemFrame fr : frames) {
                group.frameUUIDs.add(fr.getUniqueId());
                group.frameLocations.add(fr.getLocation());
            }
        }

        void onTick(long tick) {
            if (group.members.isEmpty())
                return;
            if (!videoMode && !liveMode && frames.isEmpty())
                return;
            if (tick < (startTick + warmupTicks))
                return;

            if (ticksPerFrame == -1) {
                if (frameIndex == 0 && !hasAnyPending()) {
                    byte[] linear = buffer.poll();
                    if (linear == null) {
                        if (!videoMode && !frames.isEmpty()) {
                            try {
                                File f0 = frames.get(0);
                                linear = frameSourceLoader.readFrameLinear(f0, expectedWidth(), expectedHeight(), lut);
                            } catch (IOException e) {
                                plugin.getLogger().warning("[mplay] single-frame read failed: " + e.getMessage());
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                    if (linear == null)
                        return;

                    publishFrame(linear);
                    frameIndex = 1;
                }
                return;
            }

            if (ticksPerFrame == 0) {
                if (!hasAnyPending()) {
                    byte[] linear = buffer.poll();
                    if (linear != null)
                        publishFrame(linear);
                }
                return;
            }

            if (tick >= nextFrameTick) {
                boolean ready = !buffer.isEmpty();
                if (bufferTarget > 0 && buffer.size() < bufferTarget)
                    ready = false;

                if (ready && !hasAnyPending()) {
                    byte[] linear = buffer.poll();
                    if (linear != null) {
                        publishFrame(linear);

                        if (!videoMode && !liveMode) {
                            frameIndex++;
                            if (frameIndex >= frames.size()) {
                                if (loop) {
                                    frameIndex = 0;
                                } else {
                                    stopPlayback();
                                }
                            }
                        }

                        nextFrameTick += ticksPerFrame;
                    }
                }
            }
        }

        private void publishFrame(byte[] linear) {
            int w = group.cols * 128;
            long epoch = ++group.epochCounter;

            for (int r = 0; r < group.rows; r++) {
                for (int c = 0; c < group.cols; c++) {
                    int dstIdx = r * group.cols + c;
                    Binding b = group.members.get(dstIdx);
                    byte[] tile = sliceTile(linear, w, r, c, false);
                    b.renderer.setStagedEpoch(epoch);
                    b.renderer.stageFrame(tile);
                    b.hasPendingFrame = true;
                }
            }

            List<MapView> views = new ArrayList<>(group.members.size());
            group.members.stream().sorted(Comparator.comparingInt(x -> x.mapId)).forEach(b -> views.add(b.view));

            for (Binding b : group.members)
                b.renderer.publishIfStaged();

            Map<World, List<Player>> online = playerManager.snapshotOnlineByWorld();
            World wld = group.members.get(0).world;
            List<Player> players = online.get(wld);
            if (players != null) {
                for (Player p : players) {
                    if (!inRange(p))
                        continue;
                    for (MapView v : views)
                        p.sendMap(v);
                }
            }

            for (Binding b : group.members)
                b.hasPendingFrame = false;
        }

        int loadFramesFromFolder(String folderPath) {
            if (group.members.isEmpty()) {
                plugin.getLogger().warning("No binding for screen " + id + ". Use /mplay set/create first.");
                return 0;
            }
            try {
                FrameSourceLoader.FrameLoadResult result = frameSourceLoader.loadFromFolder(
                        plugin.getDataFolder(),
                        folderPath,
                        expectedWidth(),
                        expectedHeight(),
                        lut);
                this.videoMode = result.videoMode;
                this.videoFile = result.videoFile;
                this.liveMode = false;
                this.liveUrl = null;
                this.frames = result.frameFiles;
                this.frameIndex = 0;
                this.sourceLabel = result.sourceLabel != null ? result.sourceLabel : folderPath;
                saveSessions();
                return result.frameCount;
            } catch (IOException e) {
                plugin.getLogger().warning("[mplay] screen " + id + " load failed: " + e.getMessage());
                return 0;
            }
        }

        void startLiveStream(String url, String label, int requestedTpf, int bufferFrames, CommandSender starter) {
            if (group.members.isEmpty())
                return;

            resetRenderers();
            stopPreloader();
            buffer.clear();

            this.frames = Collections.emptyList();
            this.frameIndex = 0;
            this.videoMode = false;
            this.videoFile = null;
            this.liveMode = true;
            this.liveUrl = url;
            this.sourceLabel = label != null ? label : url;
            this.loop = true; // keep ffmpeg reconnecting
            this.bufferTarget = 0;
            this.warmupTicks = 0;
            this.liveQueueLimit = (bufferFrames > 0) ? bufferFrames : DEFAULT_LIVE_QUEUE_LIMIT;
            this.frameSizeWarningLogged = false;
            this.dumpedFirstFrame = false;
            this.liveStarter = starter;
            this.firstFrameAnnounced = false;
            this.liveStartNano = System.nanoTime();
            this.liveConnectAttempts = 0;

            if (requestedTpf < 0)
                this.ticksPerFrame = -1;
            else if (requestedTpf == 0)
                this.ticksPerFrame = 0;
            else
                this.ticksPerFrame = Math.max(1, requestedTpf);

            this.startTick = MapFramePlayer.TICK;
            this.nextFrameTick = (this.ticksPerFrame <= 0)
                    ? MapFramePlayer.TICK
                    : (startTick + this.ticksPerFrame);

            startProgressNotifier();
            startPreloaderAsync();
            dbg("screen " + id + " startLiveStream url=" + url + " tpf=" + this.ticksPerFrame
                    + " liveQueueLimit=" + this.liveQueueLimit);
        }

        void startPlayback(int tpf, boolean loop, int bufferTarget, int warmupTicks) {
            if (group.members.isEmpty())
                return;

            resetRenderers();

            this.liveMode = false;
            this.liveUrl = null;
            this.liveQueueLimit = DEFAULT_LIVE_QUEUE_LIMIT;
            this.frameSizeWarningLogged = false;
            this.dumpedFirstFrame = false;

            this.ticksPerFrame = (tpf < 0) ? -1 : Math.max(1, tpf);
            this.loop = loop;
            this.bufferTarget = 0;
            this.warmupTicks = Math.max(0, warmupTicks);

            this.startTick = MapFramePlayer.TICK;
            this.nextFrameTick = (this.ticksPerFrame == -1)
                    ? Long.MAX_VALUE
                    : (startTick + this.warmupTicks + this.ticksPerFrame);

            buffer.clear();
            startPreloaderAsync();
            dbg("screen " + id + " startPlayback frames=" + frames.size()
                    + " video=" + videoMode
                    + " live=" + liveMode
                    + " tpf=" + this.ticksPerFrame
                    + " loop=" + this.loop
                    + " warmupTicks=" + this.warmupTicks);
        }

        void stopPlayback() {
            stopPreloader();
            buffer.clear();

            resetRenderers();

            frames = Collections.emptyList();
            frameIndex = 0;
            ticksPerFrame = 1;
            nextFrameTick = Long.MAX_VALUE;
            videoMode = false;
            videoFile = null;
            liveMode = false;
            liveUrl = null;
            loop = false;
            warmupTicks = 0;
            liveQueueLimit = DEFAULT_LIVE_QUEUE_LIMIT;
            frameSizeWarningLogged = false;
            dumpedFirstFrame = false;
        }

        void resetToBlack() {
            stopPlayback();
            initBlackFrame();
        }

        void clearScreen() {
            stopPlayback();
            stopPreloader();

            World w = Bukkit.getWorld(group.worldName);
            if (w != null) {
                for (UUID id : new ArrayList<>(group.frameUUIDs)) {
                    Entity e = Bukkit.getEntity(id);
                    if (e instanceof ItemFrame) {
                        ItemFrame f = (ItemFrame) e;
                        try {
                            f.setItem(null, false);
                        } catch (Throwable ignore) {
                            f.setItem(null);
                        }
                        f.remove();
                    }
                }

                if (!group.frameLocations.isEmpty()) {
                    for (Location loc : new ArrayList<>(group.frameLocations)) {
                        Location search = loc.clone();
                        search.setWorld(w);
                        for (Entity e : w.getNearbyEntities(search, 0.25, 0.25, 0.25)) {
                            if (e instanceof ItemFrame) {
                                ItemFrame f = (ItemFrame) e;
                                try {
                                    f.setItem(null, false);
                                } catch (Throwable ignore) {
                                    f.setItem(null);
                                }
                                f.remove();
                            }
                        }
                    }
                }
            }
            group.frameUUIDs.clear();
            group.frameLocations.clear();

            for (Location loc : new ArrayList<>(group.placedBarriers)) {
                Block b = loc.getBlock();
                if (b.getType() == Material.BARRIER) {
                    b.setType(Material.AIR, false);
                }
            }
            group.placedBarriers.clear();

            group.members.clear();
        }

        String describe() {
            return "screen " + id + ": " + describeBase();
        }

        String describeBrief() {
            return describe();
        }

        private String describeBase() {
            String modeInfo;
            if (liveMode)
                modeInfo = "<live>";
            else if (videoMode)
                modeInfo = "<video:" + (videoFile != null ? videoFile.getName() : "?") + ">";
            else
                modeInfo = String.valueOf(frames.size());

            String playback = (!liveMode && !videoMode && !frames.isEmpty())
                    ? " idx=" + frameIndex + "/" + Math.max(0, frames.size() - 1)
                    : "";

            String tpfInfo;
            if (ticksPerFrame == -1)
                tpfInfo = " tpf=-1";
            else if (ticksPerFrame == 0)
                tpfInfo = " tpf=source";
            else
                tpfInfo = " tpf=" + ticksPerFrame;

            String loopInfo = liveMode ? "" : " loop=" + loop;

            return "binding=" + group.members.size() + " maps layout=" + group.cols + "x" + group.rows
                    + " frames=" + modeInfo
                    + tpfInfo
                    + loopInfo
                    + playback
                    + " radius=" + maxDistance
                    + (sourceLabel.isEmpty() ? "" : (" source=" + sourceLabel));
        }

        void initBlackFrame() {
            if (group.members.isEmpty())
                return;
            int W = expectedWidth();
            byte black = MapPalette.matchColor(Color.BLACK);

            byte[] linear = new byte[W * expectedHeight()];
            Arrays.fill(linear, black);

            publishFrame(linear);
        }

        private void resetRenderers() {
            for (Binding b : group.members) {
                b.hasPendingFrame = false;
                b.scheduledSendTick = -1L;
                b.renderer.clearStagedOnly();
                b.renderer.resetSeen();
            }
        }

        private void startPreloaderAsync() {
            stopPreloader();
            preloadRunning = true;

            if (liveMode && liveUrl != null) {
                ffmpegTask = Bukkit.getScheduler().runTaskAsynchronously(plugin, this::runFfmpegLive);
                return;
            }

            if (videoMode && videoFile != null) {
                ffmpegTask = Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                    try {
                        do {
                            runFfmpegOnce();
                        } while (preloadRunning && loop);
                    } finally {
                        preloadRunning = false;
                    }
                });
                return;
            }

            preloadTask = Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    int idx = frameIndex;
                    while (preloadRunning) {
                        if (bufferTarget > 0 && buffer.size() >= bufferTarget) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException ignored) {
                            }
                            continue;
                        }
                        List<File> snap = this.frames;
                        if (snap == null || snap.isEmpty())
                            break;
                        if (idx >= snap.size()) {
                            if (loop)
                                idx = 0;
                            else
                                break;
                        }
                        File f = snap.get(idx);
                        try {
                            byte[] linear = frameSourceLoader.readFrameLinear(f, expectedWidth(), expectedHeight(),
                                    lut);
                            buffer.offer(linear);
                        } catch (IOException e) {
                            plugin.getLogger().warning("preload failed: " + f.getName() + " -> " + e.getMessage());
                        }
                        idx++;
                    }
                } finally {
                    preloadRunning = false;
                }
            });
        }

        private void stopPreloader() {
            preloadRunning = false;
            if (preloadTask != null) {
                try {
                    preloadTask.cancel();
                } catch (Throwable ignore) {
                }
                preloadTask = null;
            }
            if (ffmpegTask != null) {
                try {
                    ffmpegTask.cancel();
                } catch (Throwable ignore) {
                }
                ffmpegTask = null;
            }
            if (ffmpegProc != null) {
                try {
                    ffmpegProc.destroyForcibly();
                } catch (Throwable ignore) {
                }
                ffmpegProc = null;
            }
            if (progressTask != null) {
                try {
                    progressTask.cancel();
                } catch (Throwable ignore) {
                }
                progressTask = null;
            }
        }

        private void runFfmpegOnce() {
            final int W = expectedWidth();
            final int H = expectedHeight();
            final int RGB_BYTES = W * H * 3;

            List<String> cmd = Arrays.asList(
                    "ffmpeg",
                    "-hide_banner", "-loglevel", "error", "-nostdin",
                    "-re",
                    "-i", videoFile.getAbsolutePath(),
                    "-vf", scaleFilter(W, H),
                    "-pix_fmt", "rgb24",
                    "-f", "rawvideo", "pipe:1");

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            try {
                ffmpegProc = pb.start();
            } catch (IOException e) {
                plugin.getLogger().severe("[mplay] screen " + id + " ffmpeg launch failed: " + e.getMessage());
                return;
            }

            try (InputStream in = new BufferedInputStream(ffmpegProc.getInputStream(), RGB_BYTES * 2)) {
                byte[] rgb = new byte[RGB_BYTES];
                while (preloadRunning) {
                    if (bufferTarget > 0 && buffer.size() >= bufferTarget) {
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException ignored) {
                        }
                        continue;
                    }
                    int off = 0, n;
                    while (off < RGB_BYTES && (n = in.read(rgb, off, RGB_BYTES - off)) > 0)
                        off += n;
                    if (off < RGB_BYTES) {
                        logFrameSizeMismatch(off, RGB_BYTES, false);
                        break;
                    }
                    dumpRawFrameOnce(rgb, RGB_BYTES, false);
                    byte[] linear = frameSourceLoader.rgb24ToPalette(rgb, W, H, lut);
                    buffer.offer(linear);
                }
            } catch (IOException io) {
                plugin.getLogger().warning("[mplay] screen " + id + " ffmpeg read error: " + io.getMessage());
            } finally {
                try {
                    if (ffmpegProc != null)
                        ffmpegProc.destroy();
                } catch (Throwable ignore) {
                }
                ffmpegProc = null;
            }
        }

        private void runFfmpegLive() {
            final int W = expectedWidth();
            final int H = expectedHeight();
            final int RGB_BYTES = W * H * 3;
            while (preloadRunning) {
                List<String> cmd = new ArrayList<>();
                // escalate probing after a couple attempts
                int analyzed = (this.liveConnectAttempts >= 2) ? 1000000 : 200000;
                int probe = (this.liveConnectAttempts >= 2) ? 1000000 : 65536;
                double fps = (ticksPerFrame > 0) ? (20.0 / ticksPerFrame) : 0.0;
                this.liveConnectAttempts++;

                cmd.addAll(Arrays.asList(
                        "ffmpeg",
                        "-hide_banner", "-loglevel", "error", "-nostdin",
                        "-fflags", "nobuffer",
                        "-flags", "low_delay",
                        "-analyzeduration", String.valueOf(analyzed),
                        "-probesize", String.valueOf(probe),
                        "-reconnect", "1",
                        "-reconnect_streamed", "1",
                        "-reconnect_delay_max", "2",
                        "-i", liveUrl,
                        "-vf", scaleFilterWithFps(W, H, fps),
                        "-f", "image2pipe",
                        "-vcodec", "ppm",
                        "pipe:1"));

                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.redirectErrorStream(false);
                try {
                    ffmpegProc = pb.start();
                } catch (IOException e) {
                    plugin.getLogger().warning(
                            "[mplay] screen " + id + " live ffmpeg launch failed: " + e.getMessage());
                    if (liveStarter != null)
                        sendFeedback(liveStarter, "[mplay] live: ffmpeg start failed: " + e.getMessage());
                    sleepSilently(1000);
                    continue;
                }

                // drain stderr asynchronously to avoid blocking if ffmpeg logs
                final Process procForErr = ffmpegProc;
                Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                    try (InputStream es = procForErr.getErrorStream()) {
                        byte[] buf = new byte[1024];
                        while (preloadRunning && es.read(buf) != -1) {
                            // ignore content; loglevel=error already minimizes noise
                        }
                    } catch (IOException ignore) {
                    }
                });

                try (InputStream in = new BufferedInputStream(ffmpegProc.getInputStream(), 1 << 20)) {
                    while (preloadRunning) {
                        if (bufferTarget > 0 && buffer.size() >= bufferTarget) {
                            sleepSilently(5);
                            continue;
                        }
                        byte[] rgb = new byte[RGB_BYTES];
                        if (!readOnePPMFrame(in, W, H, rgb)) {
                            if (!firstFrameAnnounced && liveStarter != null)
                                sendFeedback(liveStarter, "[mplay] live: waiting for first frame (reconnecting if needed)...");
                            break;
                        }
                        if (!firstFrameAnnounced) {
                            firstFrameAnnounced = true;
                            long ms = (System.nanoTime() - liveStartNano) / 1_000_000L;
                            if (liveStarter != null)
                                sendFeedback(liveStarter, "[mplay] live: first frame after " + ms + " ms.");
                            if (progressTask != null) {
                                try { progressTask.cancel(); } catch (Throwable ignore) {}
                                progressTask = null;
                            }
                        }
                        dumpRawFrameOnce(rgb, RGB_BYTES, true);
                        byte[] linear = frameSourceLoader.rgb24ToPalette(rgb, W, H, lut);
                        buffer.offer(linear);
                        if (liveMode && liveQueueLimit > 0) {
                            while (buffer.size() > liveQueueLimit)
                                buffer.poll();
                        }
                    }
                } catch (IOException io) {
                    plugin.getLogger().warning(
                            "[mplay] screen " + id + " live read error: " + io.getMessage());
                    if (liveStarter != null)
                        sendFeedback(liveStarter, "[mplay] live error: " + io.getMessage());
                } finally {
                    try {
                        if (ffmpegProc != null)
                            ffmpegProc.destroy();
                    } catch (Throwable ignore) {
                    }
                    ffmpegProc = null;
                }

                if (!preloadRunning)
                    break;
                sleepSilently(1000);
            }
        }

        // Minimal PPM (P6) parser: reads one frame (binary P6 with maxval 255)
        private boolean readOnePPMFrame(InputStream in, int width, int height, byte[] out) throws IOException {
            // find magic 'P6'
            if (!seekMagicP6(in)) return false;
            String wTok = readTokenSkippingComments(in);
            String hTok = readTokenSkippingComments(in);
            String maxTok = readTokenSkippingComments(in);
            if (wTok == null || hTok == null || maxTok == null) return false;
            int w, h, maxv;
            try {
                w = Integer.parseInt(wTok);
                h = Integer.parseInt(hTok);
                maxv = Integer.parseInt(maxTok);
            } catch (NumberFormatException e) {
                return false;
            }
            if (w != width || h != height || maxv != 255) {
                // read and discard payload of this unexpected frame
                long toSkip = (long) w * (long) h * 3L;
                while (toSkip > 0) {
                    long s = in.skip(toSkip);
                    if (s <= 0) break;
                    toSkip -= s;
                }
                return false;
            }
            // now read pixel data
            int need = width * height * 3;
            int off = 0;
            while (off < need) {
                int n = in.read(out, off, need - off);
                if (n <= 0) break;
                off += n;
            }
            return off == need;
        }

        private boolean seekMagicP6(InputStream in) throws IOException {
            int prev = -1;
            int c;
            while ((c = in.read()) != -1) {
                if (prev == 'P' && c == '6') {
                    return true;
                }
                prev = c;
            }
            return false;
        }

        private String readTokenSkippingComments(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            // skip whitespace and comments
            while (true) {
                in.mark(1);
                c = in.read();
                if (c == -1) return null;
                if (Character.isWhitespace(c)) continue;
                if (c == '#') {
                    // skip until end of line
                    while ((c = in.read()) != -1 && c != '\n') {
                    }
                    continue;
                }
                in.reset();
                break;
            }
            // read token until whitespace
            while (true) {
                c = in.read();
                if (c == -1) break;
                if (Character.isWhitespace(c)) break;
                sb.append((char) c);
            }
            return sb.length() == 0 ? null : sb.toString();
        }

        private void sleepSilently(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException ignored) {
            }
        }

        private String scaleFilter(int width, int height) {
            return String.format(Locale.US,
                    "scale=%d:%d:flags=neighbor:force_original_aspect_ratio=disable,setsar=1,format=rgb24",
                    width, height);
        }

        private String scaleFilterWithFps(int width, int height, double fps) {
            String base = scaleFilter(width, height);
            if (fps > 0.0) {
                base += String.format(Locale.US, ",fps=%.3f", fps);
            }
            return base;
        }

        private void logFrameSizeMismatch(int actual, int expected, boolean live) {
            if (frameSizeWarningLogged)
                return;
            frameSizeWarningLogged = true;
            plugin.getLogger().warning(String.format(Locale.US,
                    "[mplay] screen %d %s frame truncated: read=%d expected=%d. Subsequent frames may wrap."
                            + " Enable more buffer or inspect network stability.",
                    id,
                    live ? "live" : "video",
                    actual,
                    expected));
        }

        private void dumpRawFrameOnce(byte[] rgb, int length, boolean live) {
            if (dumpedFirstFrame)
                return;
            dumpedFirstFrame = true;
            try {
                File debugDir = new File(plugin.getDataFolder(), "debug");
                if (!debugDir.exists() && !debugDir.mkdirs())
                    throw new IOException("failed to create debug directory: " + debugDir.getAbsolutePath());
                String suffix = live ? "-live" : "-video";
                File out = new File(debugDir,
                        String.format(Locale.US, "screen-%d%s-first.rgb", id, suffix));
                try (FileOutputStream fos = new FileOutputStream(out)) {
                    fos.write(Arrays.copyOf(rgb, length));
                }
                plugin.getLogger().info("[mplay] dumped raw frame to " + out.getAbsolutePath());
            } catch (IOException e) {
                plugin.getLogger().warning("[mplay] failed to dump raw frame for screen " + id + ": " + e.getMessage());
            }
        }

        private void startProgressNotifier() {
            if (liveStarter == null) return;
            if (progressTask != null) {
                try { progressTask.cancel(); } catch (Throwable ignore) {}
            }
            progressTask = Bukkit.getScheduler().runTaskTimer(plugin, () -> {
                if (!liveMode || !preloadRunning) {
                    try { progressTask.cancel(); } catch (Throwable ignore) {}
                    progressTask = null;
                    return;
                }
                if (firstFrameAnnounced) {
                    try { progressTask.cancel(); } catch (Throwable ignore) {}
                    progressTask = null;
                    return;
                }
                long ms = (System.nanoTime() - liveStartNano) / 1_000_000L;
                sendFeedback(liveStarter, "[mplay] live: connecting... " + (ms/1000.0) + "s elapsed");
            }, 40L, 60L); // start after 2s, repeat ~3s
        }

        private void dumpImageFrameOnce(BufferedImage img, boolean live) {
            if (dumpedFirstFrame)
                return;
            dumpedFirstFrame = true;
            try {
                File debugDir = new File(plugin.getDataFolder(), "debug");
                if (!debugDir.exists() && !debugDir.mkdirs())
                    throw new IOException("failed to create debug directory: " + debugDir.getAbsolutePath());
                String suffix = live ? "-live" : "-video";
                File out = new File(debugDir,
                        String.format(Locale.US, "screen-%d%s-first.png", id, suffix));
                ImageIO.write(img, "png", out);
                plugin.getLogger().info("[mplay] dumped png frame to " + out.getAbsolutePath());
            } catch (IOException e) {
                plugin.getLogger().warning("[mplay] failed to dump png frame for screen " + id + ": " + e.getMessage());
            }
        }

        private boolean hasAnyPending() {
            for (Binding b : group.members)
                if (b.hasPendingFrame)
                    return true;
            return false;
        }

        private BufferedImage ensureRGBImage(BufferedImage src, int width, int height) {
            BufferedImage current = src;
            if (src.getWidth() != width || src.getHeight() != height) {
                BufferedImage scaled = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
                Graphics2D g = scaled.createGraphics();
                try {
                    g.drawImage(src, 0, 0, width, height, null);
                } finally {
                    g.dispose();
                }
                current = scaled;
            } else if (src.getType() != BufferedImage.TYPE_INT_RGB) {
                BufferedImage converted = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
                Graphics2D g = converted.createGraphics();
                try {
                    g.drawImage(src, 0, 0, null);
                } finally {
                    g.dispose();
                }
                current = converted;
            }
            return current;
        }

        private byte[] bufferedImageToPalette(BufferedImage img, byte[] lut) {
            int width = img.getWidth();
            int height = img.getHeight();
            int[] argb = img.getRGB(0, 0, width, height, null, 0, width);
            byte[] rgb = new byte[argb.length * 3];
            int idx = 0;
            for (int pixel : argb) {
                rgb[idx++] = (byte) ((pixel >> 16) & 0xFF);
                rgb[idx++] = (byte) ((pixel >> 8) & 0xFF);
                rgb[idx++] = (byte) (pixel & 0xFF);
            }
            try {
                return frameSourceLoader.rgb24ToPalette(rgb, width, height, lut);
            } catch (IOException e) {
                throw new RuntimeException("rgb24ToPalette failed", e);
            }
        }

        private boolean inRange(Player p) {
            if (p.getWorld() != group.members.get(0).world)
                return false;
            if (this.maxDistance <= 0)
                return true;

            Location lp = p.getLocation();
            Location center = group.center != null ? group.center : p.getWorld().getSpawnLocation();

            double dx = lp.getX() - center.getX();
            double dy = lp.getY() - center.getY();
            double dz = lp.getZ() - center.getZ();
            return (dx * dx + dy * dy + dz * dz) <= (double) maxDistance * (double) maxDistance;
        }

        private int expectedWidth() {
            return group.cols * 128;
        }

        private int expectedHeight() {
            return group.rows * 128;
        }

        private int expectedBytes() {
            return expectedWidth() * expectedHeight();
        }

        private byte[] sliceTile(byte[] src, int bigW, int tileRow, int tileCol, boolean flipX) {
            byte[] out = new byte[128 * 128];
            int srcX0 = tileCol * 128;
            int srcY0 = tileRow * 128;
            if (!flipX) {
                for (int y = 0; y < 128; y++) {
                    int srcOff = (srcY0 + y) * bigW + srcX0;
                    System.arraycopy(src, srcOff, out, y * 128, 128);
                }
            } else {
                for (int y = 0; y < 128; y++) {
                    int srcOff = (srcY0 + y) * bigW + srcX0;
                    int dstOff = y * 128;
                    for (int x = 0; x < 128; x++) {
                        out[dstOff + x] = src[srcOff + (127 - x)];
                    }
                }
            }
            return out;
        }
    }
}
