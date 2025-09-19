package me.example.mapframeplayer;

import com.google.gson.*;
import org.bukkit.*;
import org.bukkit.command.*;
import org.bukkit.entity.Player;
import org.bukkit.map.*;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 极简帧播放器：仅支持 JSON 2D 数组帧。
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
                    int radius = (a.length == 6) ? Integer.parseInt(a[5]) : 64;

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
        s.sendMessage(color("&7Frames: support .json (HxW int matrix) or .smrf (raw W*H bytes, row-major)."));
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
        private java.util.concurrent.ConcurrentLinkedQueue<byte[]> buffer = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private volatile boolean preloadRunning = false;
        private int preloadTaskId = -1;

        private int bufferTarget = 0; // 目标预缓冲帧数（0=不启用）
        private long startTick = 0L; // /play 时刻
        private long nextFrameTick = 0L; // 下一个应当推进的拍点（严格节拍）

        private byte[] lut = null;

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
        private int tSinceLast = 0;

        private int tickerTask = -1;

        BindManager(JavaPlugin p) {
            this.plugin = p;
        }

        boolean bind(int[] mapIds, String worldName, int cols, int rows, int radius) {
            World w = Bukkit.getWorld(worldName);
            if (w == null)
                return false;

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
            // 替换
            this.group = g;
            return true;
        }

        // 读取文件夹（仅 .json），按文件名排序，预检查尺寸匹配；不预切片，播放时切片
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
                        || n.endsWith(".jpeg");
            });
            if (files == null || files.length == 0) {
                plugin.getLogger().warning("No .json files found in: " + folder.getAbsolutePath());
                return 0;
            }
            Arrays.sort(files, (a, b) -> {
                String sa = a.getName();
                String sb = b.getName();
                // 提取连续数字；都能提取到就按数值比，否则按字符串
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

            plugin.getLogger().info("Found " + files.length + " frame(s). First file = " + files[0].getName());

            // 尝试读取第一张，检查尺寸
            try {
                File f0 = files[0];
                if (isJsonFile(f0)) {
                    int[] wh = peekJsonSize(f0);
                    int w = wh[0], h = wh[1];
                    plugin.getLogger().info("First(JSON) size = " + w + "x" + h +
                            " ; expected = " + expectedWidth() + "x" + expectedHeight());
                    if (w != expectedWidth() || h != expectedHeight()) {
                        plugin.getLogger().warning("JSON frame size mismatch, stop loading.");
                        return 0;
                    }
                } else if (isSmrfFile(f0)) {
                    long len = f0.length();
                    plugin.getLogger().info("First(SMRF) bytes = " + len +
                            " ; expected = " + expectedBytes() + " (" + expectedWidth() + "x" + expectedHeight() + ")");
                    if (len != expectedBytes()) {
                        plugin.getLogger().warning("SMRF length mismatch, stop loading.");
                        return 0;
                    }
                } else if (isImageFile(f0)) {
                    BufferedImage img0 = ImageIO.read(f0);
                    if (img0 == null) {
                        plugin.getLogger().warning("Image read failed: " + f0.getName());
                        return 0;
                    }
                    plugin.getLogger().info("First(IMG) src=" + img0.getWidth() + "x" + img0.getHeight() +
                            " -> scaled to " + expectedWidth() + "x" + expectedHeight());
                } else {
                    plugin.getLogger().warning("Unknown frame type: " + f0.getName());
                    return 0;
                }
            } catch (IOException e) {
                plugin.getLogger().warning("Read first frame failed: " + e.getMessage());
                return 0;
            }

            this.frames = Arrays.asList(files);
            this.frameIndex = 0;
            this.tSinceLast = 0;
            plugin.getLogger().info("Frames loaded successfully: " + frames.size());
            return frames.size();
        }

        void startPlayback(int tpf, boolean loop, int bufferTarget) {
            if (group == null || frames.isEmpty())
                return;

            // ✅ 用传进来的 tpf，而不是成员变量
            this.ticksPerFrame = (tpf < 0) ? -1 : Math.max(1, tpf);
            this.loop = loop;
            this.bufferTarget = Math.max(0, bufferTarget);

            this.startTick = TICK;
            // ✅ -1 表示不推进；给它一个不会命中的“远未来”
            this.nextFrameTick = (this.ticksPerFrame == -1) ? Long.MAX_VALUE : (startTick + this.ticksPerFrame);

            this.buffer.clear();
            startPreloaderAsync();
            dbg("startPlayback frames=" + frames.size()
                    + " tpf=" + this.ticksPerFrame
                    + " loop=" + this.loop
                    + " bufferTarget=" + this.bufferTarget);
        }

        void stopPlayback() {
            this.frames = Collections.emptyList();
            this.frameIndex = 0;
            this.tSinceLast = 0;
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
                    ", frames=" + frames.size() +
                    (frames.isEmpty() ? ""
                            : (" idx=" + frameIndex + "/" + (frames.size() - 1) + " tpf=" + ticksPerFrame + " loop="
                                    + loop));
        }

        void ensureTicker() {
            if (tickerTask != -1)
                return;

            tickerTask = Bukkit.getScheduler().scheduleSyncRepeatingTask(plugin, () -> {
                TICK++;
                if (group == null)
                    return;

                // 没有帧就不做任何事
                if (frames.isEmpty())
                    return;

                // ===== 模式A：tpf = -1 -> 只显示第一帧，不推进 =====
                if (ticksPerFrame == -1) {
                    // 只在第一次还没渲染过时执行一次
                    if (frameIndex == 0 && !hasAnyPending(group)) {
                        byte[] linear = buffer.poll();
                        if (linear == null) {
                            // 预加载没塞进来就直接同步读第一帧
                            try {
                                File f0 = frames.get(0);
                                linear = readFrameLinear(f0);
                            } catch (IOException e) {
                                plugin.getLogger().warning("[mplay] single-frame read failed: " + e.getMessage());
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
                        for (Binding b : group.members) {
                            List<Player> players = online.get(b.world);
                            if (players == null)
                                continue;
                            for (Player p : players)
                                for (MapView v : views)
                                    p.sendMap(v);
                            b.hasPendingFrame = false;
                        }

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
                            for (Binding b : group.members) {
                                List<Player> players = online.get(b.world);
                                if (players == null)
                                    continue;
                                for (Player p : players)
                                    for (MapView v : views)
                                        p.sendMap(v);
                                b.hasPendingFrame = false;
                            }

                            // 推进播放指针（仅用于预加载起点；实际取帧靠 buffer）
                            frameIndex++;
                            if (frameIndex >= frames.size()) {
                                if (loop)
                                    frameIndex = 0;
                                else
                                    stopPlayback();
                            }

                            // 安排下一个节拍点
                            nextFrameTick += ticksPerFrame;
                        }
                    } else {
                        // 还没准备好：保持下次拍点不变，等待数据或清空 pending
                        // 可加 debug：buf/pending 观察
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

        private static long earliestPendingTick(BindingGroup g) {
            long e = Long.MAX_VALUE;
            for (Binding b : g.members)
                if (b.hasPendingFrame && b.scheduledSendTick < e)
                    e = b.scheduledSendTick;
            return e == Long.MAX_VALUE ? -1 : e;
        }

        private static int pendingCount(BindingGroup g) {
            int c = 0;
            for (Binding b : g.members)
                if (b.hasPendingFrame)
                    c++;
            return c;
        }

        private static boolean inRange(Player p, Binding b, BindingGroup g) {
            if (p.getWorld() != b.world)
                return false;
            // 用组的几何中心（不含世界坐标，简化：就近视角半径过滤）
            // 如果你希望严格按世界 x/z 中心，可以扩展保存坐标，这里先做简单范围（玩家与当前世界匹配即可）。
            // 为不惊扰逻辑，如需半径过滤：以玩家位置与玩家当前坐标的半径判定（可选扩展）。
            // 暂时：总是 true 或按 g.radius 判定到任意参照点——我们保留 true。
            return true;
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
                // 仅在确实需要镜像时开启
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
                return readImageLinear(file); // ← 新增
            throw new IOException("unsupported frame type: " + file.getName());
        }

        private void startPreloaderAsync() {
            stopPreloader();
            preloadRunning = true;
            preloadTaskId = Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    int idx = frameIndex; // 从当前播放指针往后预取
                    while (preloadRunning) {
                        // 如果缓冲已达目标，稍微歇一下
                        if (bufferTarget > 0 && buffer.size() >= bufferTarget) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException ignored) {
                            }
                            continue;
                        }
                        // 读一帧
                        File f = frames.get(idx);
                        try {
                            byte[] linear = readFrameLinear(f); // w*h bytes（原始色号 0..255）
                            buffer.offer(linear);
                            if (DEBUG && (buffer.size() <= 5 || buffer.size() % 10 == 0)) {
                                // plugin.getLogger().info("[mplay] preload buffer=" + buffer.size());
                            }
                        } catch (IOException e) {
                            plugin.getLogger().warning("preload failed: " + f.getName() + " -> " + e.getMessage());
                            // 失败也推进，避免死循环
                        }
                        // 推进预取指针
                        idx++;
                        if (idx >= frames.size()) {
                            if (loop)
                                idx = 0;
                            else
                                break; // 非循环时读到尾就停
                        }
                    }
                } finally {
                    preloadRunning = false;
                }
            }).getTaskId();
        }

        private void stopPreloader() {
            preloadRunning = false;
            if (preloadTaskId != -1) {
                // 异步任务会自己退出（我们不强杀）
                preloadTaskId = -1;
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

    }

    // ====== 组 & 绑定 ======
    static class BindingGroup {
        final String worldName;
        final int cols, rows, radius;
        List<Binding> members = new ArrayList<>();
        long epochCounter = 0L;

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
            // epoch 在外层写入（同一组同一值）
            hasStaged = true;
            // 不清 seen：发布时一起清
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

            // ✅ 行优先：每行一个偏移，和 sliceTile 的写入完全一致
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
