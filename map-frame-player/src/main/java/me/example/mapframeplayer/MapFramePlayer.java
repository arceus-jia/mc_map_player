package me.example.mapframeplayer;

import com.google.gson.*;
import org.bukkit.*;
import org.bukkit.command.*;
import org.bukkit.entity.Player;
import org.bukkit.map.*;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;

import java.nio.file.Files;

/**
 * 极简帧播放器：支持 JSON/SMRF/图片；当目录中只有 1 个视频文件时，自动用 ffmpeg 解码播放。
 * 命令：
 * /mplay set <id1,id2,...> <world> <cols> <rows> [radius]
 * /mplay play <folder> <ticksPerFrame> [loop] [bufferFrames]
 *
 * /mplay stop
 * /mplay status
 */
public class MapFramePlayer extends JavaPlugin implements CommandExecutor {

    // ====== 全局 Tick，用于延迟 1 tick 发包 ======
    static long TICK = 0L;

    private final BindManager binds = new BindManager(this);
    private final PlayerManager playerMgr = new PlayerManager();

    private byte[] loadLUTorFail() throws IOException {
        // 从插件数据目录读取：plugins/MapFramePlayer/colormap.lut
        File lutFile = new File(getDataFolder(), "colormap.lut");
        if (!lutFile.exists()) {
            throw new FileNotFoundException("LUT not found: " + lutFile.getAbsolutePath());
        }
        byte[] lut = Files.readAllBytes(lutFile.toPath());
        if (lut.length != 256 * 256 * 256) {
            throw new IOException("LUT size invalid: " + lut.length + " (expect 16777216 bytes)");
        }
        return lut;
    }

    @Override
    public void onEnable() {
        getCommand("mplay").setExecutor(this);
        getLogger().info("MapFramePlayer enabled.");

        try {
            byte[] lut = loadLUTorFail();
            binds.setLUT(lut);
            getLogger().info("Loaded colormap.lut (" + lut.length + " bytes).");
        } catch (IOException e) {
            getLogger().severe("Failed to load colormap.lut: " + e.getMessage());
        }

        binds.ensureTicker();
    }

    @Override
    public void onDisable() {
        binds.stop();
        getLogger().info("MapFramePlayer disabled.");
    }

    // ====== Commands ======
    @Override
    public boolean onCommand(CommandSender s, Command cmd, String label, String[] a) {
        if (!s.hasPermission("mplay.use")) {
            s.sendMessage(color("&cNo permission."));
            return true;
        }
        if (a.length == 0) {
            help(s);
            return true;
        }
        String sub = a[0].toLowerCase(Locale.ROOT);
        try {
            switch (sub) {
                case "set": {
                    // /mplay set <id1,id2,...> <world> <cols> <rows> [radius]
                    if (a.length < 5 || a.length > 6) {
                        help(s);
                        return true;
                    }
                    String[] idStr = a[1].split(",");
                    int[] ids = new int[idStr.length];
                    for (int i = 0; i < idStr.length; i++)
                        ids[i] = Integer.parseInt(idStr[i].trim());
                    String worldName = a[2];
                    int cols = Integer.parseInt(a[3]);
                    int rows = Integer.parseInt(a[4]);
                    int radius = (a.length == 6) ? Integer.parseInt(a[5]) : 0; // 0=不限距离（更友好的默认）

                    if (cols <= 0 || rows <= 0) {
                        s.sendMessage(color("&ccols/rows must > 0"));
                        return true;
                    }
                    if (ids.length != cols * rows) {
                        s.sendMessage(
                                color("&cmapId count mismatch cols*rows: " + ids.length + " != " + (cols * rows)));
                        return true;
                    }

                    boolean ok = binds.bind(ids, worldName, cols, rows, radius);
                    if (!ok) {
                        s.sendMessage(color("&cBind failed. Check world and map ids."));
                    } else {
                        s.sendMessage(color(
                                "&aBound " + ids.length + " maps. layout=" + cols + "x" + rows + " radius=" + radius));
                    }
                    return true;
                }
                case "play": {
                    // /mplay play <folder> <ticksPerFrame> [loop] [bufferFrames]
                    if (a.length < 2 || a.length > 6) {
                        help(s);
                        return true;
                    }
                    String folder = a[1];
                    int tpf = (a.length >= 3) ? Integer.parseInt(a[2]) : 1;
                    boolean loop = (a.length >= 4) ? Boolean.parseBoolean(a[3]) : false; // 默认 false
                    int bufN = (a.length >= 5) ? Integer.parseInt(a[4]) : 0; // 默认 0

                    int loaded = binds.loadFramesFromFolder(folder);
                    if (loaded <= 0) {
                        s.sendMessage(color("&cNo frames loaded. Check folder & size. " + folder));
                        return true;
                    }
                    binds.startPlayback(tpf, loop, bufN);
                    s.sendMessage(color("&aPlaying " + loaded + " frames at " + tpf + " tpf; loop=" + loop
                            + (bufN > 0 ? (" buffer=" + bufN) : "")));
                    return true;
                }

                case "stop": {
                    binds.stopPlayback();
                    s.sendMessage(color("&aPlayback stopped."));
                    return true;
                }
                case "status": {
                    s.sendMessage(color("&f" + binds.statusString()));
                    return true;
                }
                default:
                    help(s);
                    return true;
            }
        } catch (NumberFormatException e) {
            s.sendMessage(color("&cNumber format error."));
            return true;
        }
    }

    private void help(CommandSender s) {
        s.sendMessage(color("&eUsage:"));
        s.sendMessage(color("&f/mplay set <id1,id2,...> <world> <cols> <rows> [radius]"));
        s.sendMessage(color("&f/mplay play <folder> <ticksPerFrame> [loop] [bufferFrames]"));
        s.sendMessage(color("&f/mplay stop"));
        s.sendMessage(color("&f/mplay status"));
        s.sendMessage(color(
                "&7Frames: .json (HxW int), .smrf (raw W*H bytes), .png/.jpg (RGB via LUT), or a single video file (ffmpeg)."));
    }

    private static String color(String s) {
        return ChatColor.translateAlternateColorCodes('&', s);
    }

    // ====== 仅做范围过滤的玩家缓存（按世界） ======
    static class PlayerManager {
        Map<World, List<Player>> snapshotOnlineByWorld() {
            Map<World, List<Player>> m = new IdentityHashMap<>();
            for (Player p : Bukkit.getOnlinePlayers()) {
                m.computeIfAbsent(p.getWorld(), w -> new ArrayList<>()).add(p);
            }
            return m;
        }
    }

    // ====== 绑定与播放管理 ======
    static class BindManager {
        private final boolean DEBUG = true;

        // ===== 预缓冲 / 固定节拍 =====
        private final java.util.concurrent.ConcurrentLinkedQueue<byte[]> buffer = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private volatile boolean preloadRunning = false;
        private BukkitTask preloadTask = null;

        // 视频模式
        private boolean videoMode = false;
        private File videoFile = null;
        private BukkitTask ffmpegTask = null;
        private Process ffmpegProc = null;

        private int bufferTarget = 0; // 目标预缓冲帧数（0=不启用）
        private long startTick = 0L; // /play 时刻
        private long nextFrameTick = 0L; // 下一个应当推进的拍点（严格节拍）

        private byte[] lut = null;

        // —— 最大播放距离（<=0 表示无限制），取世界出生点为中心 ——
        private int maxDistance = 0;

        void setLUT(byte[] lut) {
            this.lut = lut;
        }

        private void dbg(String msg) {
            if (DEBUG)
                plugin.getLogger().info("[mplay] " + msg);
        }

        private final JavaPlugin plugin;

        // 单组（极简）：一墙 N 张图
        private BindingGroup group;

        // 播放状态
        private List<File> frames = Collections.emptyList();
        private int frameIndex = 0;
        private int ticksPerFrame = 2;
        private boolean loop = false;

        private int tickerTask = -1;

        BindManager(JavaPlugin p) {
            this.plugin = p;
        }

        boolean bind(int[] mapIds, String worldName, int cols, int rows, int radius) {
            World w = Bukkit.getWorld(worldName);
            if (w == null)
                return false;

            // 记录最大播放距离到一个字段（中心点用世界出生点）
            this.maxDistance = radius;

            // 构建组（替换旧组）
            BindingGroup g = new BindingGroup(worldName, cols, rows, radius);
            g.members = new ArrayList<>(mapIds.length);

            int idx = 0;
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    int mapId = mapIds[idx++];
                    MapView view = Bukkit.getMap(mapId);
                    if (view == null)
                        return false;

                    // 清理并挂上 DataRenderer
                    for (MapRenderer r0 : new ArrayList<>(view.getRenderers()))
                        view.removeRenderer(r0);
                    DataRenderer renderer = new DataRenderer();
                    view.setScale(MapView.Scale.NORMAL);
                    view.setTrackingPosition(false);
                    view.setUnlimitedTracking(false);
                    view.addRenderer(renderer);

                    g.members.add(new Binding(mapId, w, view, renderer));
                }
            }
            g.center = w.getPlayers().isEmpty() ? w.getSpawnLocation() : w.getPlayers().get(0).getLocation();
            // 替换
            this.group = g;
            return true;
        }

        // 读取文件夹（.json/.smrf/.png/.jpg/.jpeg），或仅 1 个视频文件；按文件名排序，预检查尺寸匹配（图片/帧）
        int loadFramesFromFolder(String folderPath) {
            if (group == null) {
                plugin.getLogger().warning("No group bound. Use /mplay set first.");
                return 0;
            }
            File folder = new File(plugin.getDataFolder(), "frames/" + folderPath);

            plugin.getLogger().info("Trying to load frames from: " + folder.getAbsolutePath());
            if (!folder.exists()) {
                plugin.getLogger().warning("Folder does not exist: " + folder.getAbsolutePath());
                return 0;
            }
            if (!folder.isDirectory()) {
                plugin.getLogger().warning("Path is not a directory: " + folder.getAbsolutePath());
                return 0;
            }

            File[] files = folder.listFiles((dir, name) -> {
                String n = name.toLowerCase(Locale.ROOT);
                return n.endsWith(".json") || n.endsWith(".smrf") || n.endsWith(".png") || n.endsWith(".jpg")
                        || n.endsWith(".jpeg") || isVideoName(n);
            });
            if (files == null || files.length == 0) {
                plugin.getLogger().warning("No frame files found in: " + folder.getAbsolutePath());
                return 0;
            }

            // 若目录里只有一个视频文件 -> 视频模式
            if (files.length == 1 && isVideoFile(files[0])) {
                this.videoMode = true;
                this.videoFile = files[0];
                this.frames = Collections.emptyList();
                this.frameIndex = 0;
                plugin.getLogger().info("[mplay] Video mode: " + videoFile.getName());
                return 1;
            }

            // 过滤出非视频文件
            List<File> list = new ArrayList<>();
            for (File f : files)
                if (!isVideoFile(f))
                    list.add(f);
            File[] frameFiles = list.toArray(new File[0]);

            Arrays.sort(frameFiles, (a, b) -> {
                String sa = a.getName();
                String sb = b.getName();
                String ra = sa.replaceAll("\\D+", "");
                String rb = sb.replaceAll("\\D+", "");
                if (!ra.isEmpty() && !rb.isEmpty()) {
                    try {
                        int ia = Integer.parseInt(ra);
                        int ib = Integer.parseInt(rb);
                        return Integer.compare(ia, ib);
                    } catch (NumberFormatException ignore) {
                    }
                }
                return sa.compareTo(sb);
            });

            plugin.getLogger().info("Found " + frameFiles.length + " frame(s). First = " + frameFiles[0].getName());

            // 尝试读取第一张，检查尺寸（仅对 JSON/SMRF/IMG）
            try {
                File f0 = frameFiles[0];
                if (isJsonFile(f0)) {
                    int[] wh = peekJsonSize(f0);
                    int w = wh[0], h = wh[1];
                    if (w != expectedWidth() || h != expectedHeight()) {
                        plugin.getLogger().warning("JSON frame size mismatch.");
                        return 0;
                    }
                } else if (isSmrfFile(f0)) {
                    long len = f0.length();
                    if (len != expectedBytes()) {
                        plugin.getLogger().warning("SMRF length mismatch.");
                        return 0;
                    }
                } else if (isImageFile(f0)) {
                    BufferedImage img0 = ImageIO.read(f0);
                    if (img0 == null) {
                        plugin.getLogger().warning("Image read failed: " + f0.getName());
                        return 0;
                    }
                } else {
                    plugin.getLogger().warning("Unknown frame type: " + f0.getName());
                    return 0;
                }
            } catch (IOException e) {
                plugin.getLogger().warning("Read first frame failed: " + e.getMessage());
                return 0;
            }

            this.videoMode = false;
            this.videoFile = null;
            this.frames = Arrays.asList(frameFiles);
            this.frameIndex = 0;
            plugin.getLogger().info("Frames loaded successfully: " + frames.size());
            return frames.size();
        }

        void startPlayback(int tpf, boolean loop, int bufferTarget) {
            if (group == null)
                return;

            // 清 pending/staged，确保无残留
            for (Binding b : group.members) {
                b.hasPendingFrame = false;
                b.scheduledSendTick = -1L;
                b.renderer.clearStagedOnly();
                b.renderer.resetSeen();
            }

            // ✅ 用传进来的 tpf（允许 -1 定格）
            this.ticksPerFrame = (tpf < 0) ? -1 : Math.max(1, tpf);
            this.loop = loop;
            this.bufferTarget = Math.max(0, bufferTarget);

            this.startTick = TICK;
            this.nextFrameTick = (this.ticksPerFrame == -1) ? Long.MAX_VALUE : (startTick + this.ticksPerFrame);

            this.buffer.clear();
            startPreloaderAsync();
            dbg("startPlayback frames=" + frames.size()
                    + " video=" + videoMode
                    + " tpf=" + this.ticksPerFrame
                    + " loop=" + this.loop
                    + " bufferTarget=" + this.bufferTarget);
        }

        void stopPlayback() {
            // 停止预加载线程 + 清空缓冲（先停任务，再改 frames，避免竞态）
            stopPreloader();
            buffer.clear();

            if (group != null) {
                for (Binding b : group.members) {
                    b.hasPendingFrame = false;
                    b.scheduledSendTick = -1L;
                    b.renderer.clearStagedOnly();
                    b.renderer.resetSeen();
                }
            }

            this.frames = Collections.emptyList();
            this.frameIndex = 0;

            this.ticksPerFrame = 1;
            this.nextFrameTick = Long.MAX_VALUE;
        }

        void stop() {
            stopPlayback();
            if (tickerTask != -1) {
                Bukkit.getScheduler().cancelTask(tickerTask);
                tickerTask = -1;
            }
        }

        String statusString() {
            if (group == null)
                return "no binding";
            return "binding: " + (group.members.size()) + " maps, layout=" + group.cols + "x" + group.rows +
                    ", frames="
                    + (videoMode ? ("<video:" + (videoFile != null ? videoFile.getName() : "?") + ">")
                            : String.valueOf(frames.size()))
                    +
                    (videoMode ? ""
                            : (frames.isEmpty() ? ""
                                    : (" idx=" + frameIndex + "/" + (frames.size() - 1) + " tpf=" + ticksPerFrame
                                            + " loop=" + loop)))
                    +
                    ", maxDist=" + maxDistance;
        }

        void ensureTicker() {
            if (tickerTask != -1)
                return;

            tickerTask = Bukkit.getScheduler().scheduleSyncRepeatingTask(plugin, () -> {
                TICK++;
                if (group == null)
                    return;

                // 没有帧且非视频模式就不做任何事
                if (!videoMode && frames.isEmpty())
                    return;

                // ===== 模式A：tpf = -1 -> 只显示第一帧，不推进 =====
                if (ticksPerFrame == -1) {
                    if (frameIndex == 0 && !hasAnyPending(group)) {
                        byte[] linear = buffer.poll();
                        if (linear == null) {
                            // 预加载没塞进来就直接同步读第一帧（仅非视频时有效）
                            if (!videoMode) {
                                try {
                                    File f0 = frames.get(0);
                                    linear = readFrameLinear(f0);
                                } catch (IOException e) {
                                    plugin.getLogger().warning("[mplay] single-frame read failed: " + e.getMessage());
                                    return;
                                }
                            } else {
                                // 视频模式还没缓冲到首帧就等下一 tick
                                return;
                            }
                        }
                        if (linear == null)
                            return;

                        int w = group.cols * 128;
                        long epoch = ++group.epochCounter;

                        // stage 所有 tile
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

                        // 同 tick 内统一发布 + 发送
                        List<MapView> views = new ArrayList<>(group.members.size());
                        group.members.stream().sorted(Comparator.comparingInt(x -> x.mapId))
                                .forEach(b -> views.add(b.view));

                        for (Binding b : group.members)
                            b.renderer.publishIfStaged();

                        Map<World, List<Player>> online = new IdentityHashMap<>();
                        for (Player p : Bukkit.getOnlinePlayers()) {
                            online.computeIfAbsent(p.getWorld(), wld -> new ArrayList<>()).add(p);
                        }
                        // 以玩家为主循环，做距离过滤
                        World wld = group.members.get(0).world;
                        List<Player> players = online.get(wld);
                        if (players != null) {
                            for (Player p : players) {
                                // 任取一个 binding 判断世界 & 距离（同一世界）
                                Binding any = group.members.get(0);
                                if (!this.inRange(p, any, group))
                                    continue;
                                for (MapView v : views)
                                    p.sendMap(v);
                            }
                        }

                        for (Binding b : group.members)
                            b.hasPendingFrame = false;

                        // 标记“已处理过第一帧”，后续不再重复
                        frameIndex = 1;
                    }
                    return; // tpf = -1 情况下结束本 tick
                }

                // ===== 模式B：正常固定节拍推进 =====
                if (TICK >= nextFrameTick) {
                    boolean ready = !buffer.isEmpty();
                    if (bufferTarget > 0 && buffer.size() < bufferTarget)
                        ready = false;

                    if (ready && !hasAnyPending(group)) {
                        byte[] linear = buffer.poll();
                        if (linear != null) {
                            int w = group.cols * 128;
                            long epoch = ++group.epochCounter;

                            // stage 所有 tile
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

                            // 同 tick 内统一发布 + 发送
                            List<MapView> views = new ArrayList<>(group.members.size());
                            group.members.stream().sorted(Comparator.comparingInt(x -> x.mapId))
                                    .forEach(b -> views.add(b.view));

                            for (Binding b : group.members)
                                b.renderer.publishIfStaged();

                            Map<World, List<Player>> online = new IdentityHashMap<>();
                            for (Player p : Bukkit.getOnlinePlayers()) {
                                online.computeIfAbsent(p.getWorld(), wld -> new ArrayList<>()).add(p);
                            }
                            World wld = group.members.get(0).world;
                            List<Player> players = online.get(wld);
                            if (players != null) {
                                for (Player p : players) {
                                    Binding any = group.members.get(0);
                                    if (!this.inRange(p, any, group))
                                        continue;
                                    for (MapView v : views)
                                        p.sendMap(v);
                                }
                            }
                            for (Binding b : group.members)
                                b.hasPendingFrame = false;

                            // 推进播放指针（仅用于预加载起点；实际取帧靠 buffer）
                            if (!videoMode) {
                                frameIndex++;
                                if (frameIndex >= frames.size()) {
                                    if (loop)
                                        frameIndex = 0;
                                    else
                                        stopPlayback();
                                }
                            }

                            // 安排下一个节拍点
                            nextFrameTick += ticksPerFrame;
                        }
                    } else {
                        // 还没准备好：保持下次拍点不变，等待数据或清空 pending
                    }
                }
            }, 1L, 1L);
        }

        private static boolean hasAnyPending(BindingGroup g) {
            for (Binding b : g.members)
                if (b.hasPendingFrame)
                    return true;
            return false;
        }

        private static byte[] sliceTile(byte[] src, int bigW, int tileRow, int tileCol, boolean flipX) {
            byte[] out = new byte[128 * 128];
            int srcX0 = tileCol * 128, srcY0 = tileRow * 128;
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

        // ======== 文件类型判断 ========
        private static boolean isJsonFile(File f) {
            String n = f.getName().toLowerCase(Locale.ROOT);
            return n.endsWith(".json");
        }

        private static boolean isSmrfFile(File f) {
            String n = f.getName().toLowerCase(Locale.ROOT);
            return n.endsWith(".smrf");
        }

        private static boolean isImageFile(File f) {
            String n = f.getName().toLowerCase(Locale.ROOT);
            return n.endsWith(".png") || n.endsWith(".jpg") || n.endsWith(".jpeg");
        }

        private static boolean isVideoFile(File f) {
            return isVideoName(f.getName().toLowerCase(Locale.ROOT));
        }

        private static boolean isVideoName(String n) {
            return n.endsWith(".mp4") || n.endsWith(".mov") || n.endsWith(".m4v")
                    || n.endsWith(".avi") || n.endsWith(".webm") || n.endsWith(".wmv")
                    || n.endsWith(".ts") || n.endsWith(".m3u8") || n.endsWith(".gif");
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

        private int[] peekJsonSize(File file) throws IOException {
            try (Reader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                JsonArray arr = JsonParser.parseReader(r).getAsJsonArray();
                int h = arr.size();
                if (h == 0)
                    throw new IOException("empty array");
                int w = arr.get(0).getAsJsonArray().size();
                return new int[] { w, h };
            }
        }

        private byte[] readJsonLinear(File file) throws IOException {
            try (Reader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                JsonArray arr = JsonParser.parseReader(r).getAsJsonArray();
                int h = arr.size();
                if (h == 0)
                    throw new IOException("empty array");
                JsonArray row0 = arr.get(0).getAsJsonArray();
                int w = row0.size();
                if (group == null)
                    throw new IOException("no binding");
                if (w != group.cols * 128 || h != group.rows * 128) {
                    throw new IOException("frame size mismatch: " + w + "x" + h);
                }
                byte[] out = new byte[w * h];
                int pos = 0;
                for (int y = 0; y < h; y++) {
                    JsonArray row = arr.get(y).getAsJsonArray();
                    if (row.size() != w)
                        throw new IOException("bad row width @y=" + y);
                    for (int x = 0; x < w; x++) {
                        int v = row.get(x).getAsInt();
                        out[pos++] = (byte) (v & 0xFF);
                    }
                }
                return out;
            }
        }

        private byte[] readSmrfLinear(File file) throws IOException {
            int need = expectedBytes();
            try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                byte[] buf = in.readNBytes(need);
                if (buf.length != need) {
                    throw new IOException("smrf length mismatch: got=" + buf.length + " need=" + need);
                }
                return buf;
            }
        }

        /** 统一入口：根据扩展名读取为线性 W*H 字节数组 */
        private byte[] readFrameLinear(File file) throws IOException {
            if (isJsonFile(file))
                return readJsonLinear(file);
            if (isSmrfFile(file))
                return readSmrfLinear(file);
            if (isImageFile(file))
                return readImageLinear(file); // 图片 -> LUT
            throw new IOException("unsupported frame type: " + file.getName());
        }

        private void startPreloaderAsync() {
            stopPreloader(); // 确保只有一个任务
            preloadRunning = true;

            if (videoMode && videoFile != null) {
                // —— 视频模式：启动 ffmpeg 解码为 rgb24 流，再映射到索引并塞入 buffer ——
                ffmpegTask = Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                    try {
                        do {
                            runFfmpegOnce(); // 播一次（或直到进程退出）
                        } while (preloadRunning && loop);
                    } finally {
                        preloadRunning = false;
                    }
                });
                return;
            }

            // —— 非视频：原来的逐文件预读 ——
            preloadTask = Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    int idx = frameIndex; // 从当前播放指针往后预取
                    while (preloadRunning) {
                        if (bufferTarget > 0 && buffer.size() >= bufferTarget) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException ignored) {
                            }
                            continue;
                        }
                        // 局部快照，防止 frames 被置空导致越界
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
                            byte[] linear = readFrameLinear(f); // w*h bytes（原始色号 0..255）
                            buffer.offer(linear);
                        } catch (IOException e) {
                            plugin.getLogger().warning("preload failed: " + f.getName() + " -> " + e.getMessage());
                            // 失败也推进，避免死循环
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
        }

        /** 质量不错的等比缩放到目标尺寸（不保留透明度，RGB 即可） */
        private static BufferedImage resizeImage(BufferedImage src, int dstW, int dstH) {
            BufferedImage dst = new BufferedImage(dstW, dstH, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2 = dst.createGraphics();
            try {
                g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
                g2.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                g2.drawImage(src, 0, 0, dstW, dstH, null);
            } finally {
                g2.dispose();
            }
            return dst;
        }

        /** 把图片读入并缩放到 W×H，然后用 LUT 映射为 W*H 的调色板索引（uint8） */
        private byte[] readImageLinear(File file) throws IOException {
            if (lut == null) {
                throw new IOException("LUT not loaded; put colormap.lut under plugins/MapFramePlayer/");
            }
            BufferedImage img = ImageIO.read(file);
            if (img == null) {
                throw new IOException("ImageIO.read returned null for " + file.getName());
            }
            final int W = expectedWidth();
            final int H = expectedHeight();
            BufferedImage scaled = (img.getWidth() == W && img.getHeight() == H)
                    ? img
                    : resizeImage(img, W, H);

            int[] rgb = scaled.getRGB(0, 0, W, H, null, 0, W);
            byte[] out = new byte[W * H];
            // 行优先：与 JSON/SMRF 保持一致
            for (int i = 0; i < rgb.length; i++) {
                int p = rgb[i];
                int r = (p >>> 16) & 0xFF;
                int g = (p >>> 8) & 0xFF;
                int b = p & 0xFF;
                int key = (r << 16) | (g << 8) | b; // 与 Python 导出的 C-order LUT 对齐
                out[i] = lut[key]; // 直接是 0..255（Java byte 有符号，后续使用时注意 & 0xFF）
            }
            return out;
        }

        // ==== ffmpeg：视频一次播放（向 buffer 推入映射后的线性帧） ====
        private void runFfmpegOnce() {
            // 用当前 tpf 推导 fps（20 tick/s）；tpf=-1（定格）时也按 20/1 处理，保证有帧
            int tpfLocal = (this.ticksPerFrame <= 0) ? 1 : this.ticksPerFrame;
            double fps = 20.0 / tpfLocal;

            final int W = expectedWidth();
            final int H = expectedHeight();
            final int RGB_BYTES = W * H * 3;

            List<String> cmd = Arrays.asList(
                    "ffmpeg",
                    "-hide_banner", "-loglevel", "error", "-nostdin",
                    "-re", // 按实时速率读，画面更稳；如需更快填充可去掉
                    "-i", videoFile.getAbsolutePath(),
                    "-vf", "scale=" + W + ":" + H + ":flags=neighbor",
                    "-r", String.format(Locale.US, "%.6f", fps),
                    "-pix_fmt", "rgb24",
                    "-f", "rawvideo", "pipe:1");

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            try {
                ffmpegProc = pb.start();
            } catch (IOException e) {
                plugin.getLogger().severe("[mplay] ffmpeg launch failed: " + e.getMessage());
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
                    // 读一帧
                    int off = 0, n;
                    while (off < RGB_BYTES && (n = in.read(rgb, off, RGB_BYTES - off)) > 0)
                        off += n;
                    if (off < RGB_BYTES)
                        break; // EOF
                    // 映射到地图调色板
                    byte[] linear = rgb24ToPalette(rgb, W, H);
                    buffer.offer(linear);
                }
            } catch (IOException io) {
                plugin.getLogger().warning("[mplay] ffmpeg read error: " + io.getMessage());
            } finally {
                try {
                    if (ffmpegProc != null)
                        ffmpegProc.destroy();
                } catch (Throwable ignore) {
                }
                ffmpegProc = null;
            }
        }

        private byte[] rgb24ToPalette(byte[] rgb, int W, int H) {
            if (lut == null)
                throw new IllegalStateException("LUT not loaded");
            byte[] out = new byte[W * H];
            int p = 0; // rgb 输入指针
            for (int i = 0; i < out.length; i++) {
                int r = rgb[p++] & 0xFF;
                int g = rgb[p++] & 0xFF;
                int b = rgb[p++] & 0xFF;
                int key = (r << 16) | (g << 8) | b;
                out[i] = lut[key];
            }
            return out;
        }

        // ===== 距离过滤：以世界出生点为中心；maxDistance<=0 即不限 =====
        private boolean inRange(Player p, Binding b, BindingGroup g) {
            if (p.getWorld() != b.world)
                return false;
            if (this.maxDistance <= 0)
                return true;

            Location lp = p.getLocation();
            Location center = g.center != null ? g.center : b.world.getSpawnLocation();

            double dx = lp.getX() - center.getX();
            double dy = lp.getY() - center.getY();
            double dz = lp.getZ() - center.getZ();
            return (dx * dx + dy * dy + dz * dz) <= (double) maxDistance * (double) maxDistance;
        }

    }

    // ====== 组 & 绑定 ======
    static class BindingGroup {
        final String worldName;
        final int cols, rows, radius;
        List<Binding> members = new ArrayList<>();
        long epochCounter = 0L;
        Location center;

        BindingGroup(String worldName, int cols, int rows, int radius) {
            this.worldName = worldName;
            this.cols = cols;
            this.rows = rows;
            this.radius = radius;
        }
    }

    static class Binding {
        final int mapId;
        final World world;
        final MapView view;
        final DataRenderer renderer;

        long scheduledSendTick = -1L;
        boolean hasPendingFrame = false;

        Binding(int id, World w, MapView v, DataRenderer r) {
            this.mapId = id;
            this.world = w;
            this.view = v;
            this.renderer = r;
        }
    }

    // ====== 数据驱动渲染器（不扫世界） ======
    static class DataRenderer extends MapRenderer {
        private final byte[] current = new byte[128 * 128];
        private long currentEpoch = 0L;

        private final byte[] staged = new byte[128 * 128];
        private long stagedEpoch = 0L;
        private boolean hasStaged = false;

        private final Map<UUID, Long> seen = new HashMap<>();

        DataRenderer() {
            super(true);
        }

        // 播放线程：只把帧暂存，不增 epoch，不影响当前已展示帧
        synchronized void stageFrame(byte[] pixels128) {
            System.arraycopy(pixels128, 0, staged, 0, 128 * 128);
            hasStaged = true;
        }

        synchronized void setStagedEpoch(long epoch) {
            this.stagedEpoch = epoch;
        }

        // 发布：原子切换到 staged（在同一 tick 内，组里全部调用）
        synchronized void publishIfStaged() {
            if (!hasStaged)
                return;
            System.arraycopy(staged, 0, current, 0, 128 * 128);
            currentEpoch = stagedEpoch;
            hasStaged = false;
            seen.clear(); // 所有玩家需要重画
        }

        synchronized void clearStagedOnly() {
            hasStaged = false;
            stagedEpoch = 0L;
        }

        synchronized void resetSeen() {
            seen.clear();
        }

        @Override
        public void render(MapView map, MapCanvas canvas, Player player) {
            long sv = seen.getOrDefault(player.getUniqueId(), -1L);
            long epoch;
            byte[] src;
            synchronized (this) {
                epoch = currentEpoch;
                src = current;
            }
            if (sv == epoch)
                return;

            for (int y = 0; y < 128; y++) {
                int rowOff = y * 128;
                for (int x = 0; x < 128; x++) {
                    canvas.setPixel(x, y, src[rowOff + x]);
                }
            }
            seen.put(player.getUniqueId(), epoch);
        }

    }
}
