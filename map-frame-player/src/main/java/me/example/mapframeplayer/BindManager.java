package me.example.mapframeplayer;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockFace;
import org.bukkit.entity.Entity;
import org.bukkit.entity.ItemFrame;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.MapMeta;
import org.bukkit.map.MapPalette;
import org.bukkit.map.MapRenderer;
import org.bukkit.map.MapView;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

class BindManager {
    private final boolean DEBUG = true;

    // ===== 预缓冲 / 固定节拍 =====
    private final ConcurrentLinkedQueue<byte[]> buffer = new ConcurrentLinkedQueue<>();
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

    private int warmupTicks = 0;

    void setLUT(byte[] lut) {
        this.lut = lut;
    }

    private void dbg(String msg) {
        if (DEBUG)
            plugin.getLogger().info("[mplay] " + msg);
    }

    private final JavaPlugin plugin;
    private final PlayerManager playerManager;

    // 单组（极简）：一墙 N 张图
    private BindingGroup group;

    // 播放状态
    private List<File> frames = Collections.emptyList();
    private int frameIndex = 0;
    private int ticksPerFrame = 2;
    private boolean loop = false;

    private int tickerTask = -1;

    BindManager(JavaPlugin p, PlayerManager playerManager) {
        this.plugin = p;
        this.playerManager = playerManager;
    }

    /** 把当前绑定组初始化为纯黑画面（不等 tick，立刻发布并发送一次） */
    private void initBlackFrame() {
        if (group == null || group.members.isEmpty())
            return;
        int W = expectedWidth(), H = expectedHeight();
        byte black = MapPalette.matchColor(Color.BLACK);

        byte[] linear = new byte[W * H];
        Arrays.fill(linear, black);

        int bigW = group.cols * 128;
        long epoch = ++group.epochCounter;

        // stage tiles
        for (int r = 0; r < group.rows; r++) {
            for (int c = 0; c < group.cols; c++) {
                int idx = r * group.cols + c;
                Binding b = group.members.get(idx);
                byte[] tile = sliceTile(linear, bigW, r, c, false);
                b.renderer.setStagedEpoch(epoch);
                b.renderer.stageFrame(tile);
                b.hasPendingFrame = true;
            }
        }

        // publish
        List<MapView> views = new ArrayList<>(group.members.size());
        group.members.stream().sorted(Comparator.comparingInt(x -> x.mapId)).forEach(b -> views.add(b.view));
        for (Binding b : group.members)
            b.renderer.publishIfStaged();

        // send to in-range players
        Map<World, List<Player>> online = playerManager.snapshotOnlineByWorld();
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
    }

    // 如无注解依赖，可去掉 @Nullable
    boolean bind(int[] mapIds, String worldName, int cols, int rows, int radius /* , @Nullable Location center */ ,
            Location center) {
        World w = Bukkit.getWorld(worldName);
        if (w == null)
            return false;

        // 记录半径
        this.maxDistance = radius;

        // 计算中心点（优先用传入的 center，其次用该世界的 spawn）
        Location centerLoc = (center != null && center.getWorld() == w) ? center.clone() : w.getSpawnLocation();

        // 构建组
        BindingGroup g = new BindingGroup(worldName, cols, rows, radius);
        g.center = centerLoc; // ← 先把中心放进去
        g.members = new ArrayList<>(mapIds.length);

        int idx = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                int mapId = mapIds[idx++];
                MapView view = Bukkit.getMap(mapId);
                if (view == null)
                    return false;

                for (MapRenderer r0 : new ArrayList<>(view.getRenderers()))
                    view.removeRenderer(r0);
                DataRenderer renderer = new DataRenderer();
                view.setScale(MapView.Scale.NORMAL);
                view.setTrackingPosition(false);
                view.setUnlimitedTracking(false);
                view.addRenderer(renderer);

                // 把范围信息灌进去（用已经确定的 centerLoc）
                renderer.setRange(w, centerLoc, this.maxDistance);

                g.members.add(new Binding(mapId, w, view, renderer));
            }
        }

        // 替换当前组
        this.group = g;
        return true;
    }

    /** 在玩家面前 forward 格生成屏障墙 + 展示框 + 地图，并绑定，最后渲染纯黑初始画面 */
    boolean createAndBindFloatingAtPlayer(Player p, int cols, int rows, int radius, int forward, Location anchor) {
        World w = p.getWorld();
        BlockFace face = p.getFacing(); // 屏幕朝向玩家
        BlockFace right = rightOf(face); // 屏幕向右的方向

        // 屏幕平面左下角：优先用 anchor；否则仍用“面前 forward 格”
        Block base = (anchor != null && anchor.getWorld() == w)
                ? w.getBlockAt(anchor.getBlockX(), anchor.getBlockY(), anchor.getBlockZ())
                : p.getLocation().getBlock().getRelative(face, Math.max(1, forward));

        Block backingBase = base.getRelative(face); // 屏幕后退一格作为背板列（BARRIER）

        // 1) 屏障（只在空气处放），上->下、左->右
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

        // 2) Map + ItemFrame（保持你原有朝向与顺序）
        int[] ids = new int[cols * rows];
        int idx = 0;
        List<ItemFrame> frames = new ArrayList<>();
        for (int r = 0; r < rows; r++) {
            int yOff = (rows - 1 - r);
            for (int c = 0; c < cols; c++) {
                Block backing = base.getRelative(right, c).getRelative(BlockFace.UP, yOff); // 注意：这里仍用 base（屏幕平面）
                Location floc = backing.getLocation().add(0.5, 0.5, 0.5);

                MapView view = Bukkit.createMap(w);
                ItemStack mapItem = new ItemStack(Material.FILLED_MAP, 1);
                MapMeta meta = (MapMeta) mapItem.getItemMeta();
                if (meta != null) {
                    meta.setMapView(view);
                    mapItem.setItemMeta(meta);
                }

                ItemFrame frame = w.spawn(floc, ItemFrame.class, f -> {
                    BlockFace attachFace = face.getOppositeFace(); // ← 保持你原版
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
                    for (ItemFrame fr : frames)
                        try {
                            fr.remove();
                        } catch (Throwable ignore) {
                        }
                    for (Block b : placedBarriers)
                        try {
                            b.setType(Material.AIR, false);
                        } catch (Throwable ignore) {
                        }
                    plugin.getLogger().warning("[mplay] failed to get map id: " + t.getMessage());
                    return false;
                }
            }
        }

        // 3) 半径中心：改为“屏幕几何中心”，更准确
        // 计算 right 的水平偏移量
        int rx = right.getModX(), rz = right.getModZ();
        double cx = base.getX() + 0.5 + rx * ((cols - 1) / 2.0);
        double cy = base.getY() + 0.5 + ((rows - 1) / 2.0);
        double cz = base.getZ() + 0.5 + rz * ((cols - 1) / 2.0);
        Location center = new Location(w, cx, cy, cz);

        boolean bound = bind(ids, w.getName(), cols, rows, radius, center);
        if (!bound) {
            for (ItemFrame fr : frames)
                try {
                    fr.remove();
                } catch (Throwable ignore) {
                }
            for (Block b : placedBarriers)
                try {
                    b.setType(Material.AIR, false);
                } catch (Throwable ignore) {
                }
            return false;
        }

        // 4) 记录清理用对象（/mplay clear）
        if (this.group != null) {
            this.group.placedBarriers.clear();
            for (Block b : placedBarriers)
                this.group.placedBarriers.add(b.getLocation());
            this.group.frameUUIDs.clear();
            for (ItemFrame fr : frames)
                this.group.frameUUIDs.add(fr.getUniqueId());
        }

        // 5) 初始化黑屏
        initBlackFrame();
        return true;
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

    void startPlayback(int tpf, boolean loop, int bufferTarget, int warmupTicks) {
        if (group == null)
            return;

        // 清 pending/staged（保持你原有代码）
        for (Binding b : group.members) {
            b.hasPendingFrame = false;
            b.scheduledSendTick = -1L;
            b.renderer.clearStagedOnly();
            b.renderer.resetSeen();
        }

        this.ticksPerFrame = (tpf < 0) ? -1 : Math.max(1, tpf);
        this.loop = loop;

        // 旧的“攒 N 帧再开播”策略禁用：直接归零（也可保留为你想要的最小缓存）
        this.bufferTarget = 0;

        // 新增：预热 tick（可控起播延迟）
        this.warmupTicks = Math.max(0, warmupTicks);

        this.startTick = MapFramePlayer.TICK;
        // 注意：首帧推进点 = start + warmup + tpf
        this.nextFrameTick = (this.ticksPerFrame == -1)
                ? Long.MAX_VALUE
                : (startTick + this.warmupTicks + this.ticksPerFrame);

        this.buffer.clear();
        startPreloaderAsync();
        dbg("startPlayback frames=" + frames.size()
                + " video=" + videoMode
                + " tpf=" + this.ticksPerFrame
                + " loop=" + this.loop
                + " warmupTicks=" + this.warmupTicks);
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
            MapFramePlayer.TICK++;
            if (group == null)
                return;

            // 没有帧且非视频模式就不做任何事
            if (!videoMode && frames.isEmpty())
                return;

            if (MapFramePlayer.TICK < (startTick + warmupTicks))
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

                    Map<World, List<Player>> online = playerManager.snapshotOnlineByWorld();
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
            if (MapFramePlayer.TICK >= nextFrameTick) {
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

                        Map<World, List<Player>> online = playerManager.snapshotOnlineByWorld();
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

    boolean clearScreen() {
        // 停播放，先不把 group 置空，方便读信息
        stopPlayback();
        if (group == null)
            return false;

        // 1) 移除 ItemFrame（不掉落地图）
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
        }
        group.frameUUIDs.clear();

        // 2) 恢复我们放的屏障
        for (Location loc : new ArrayList<>(group.placedBarriers)) {
            Block b = loc.getBlock();
            if (b.getType() == Material.BARRIER) {
                b.setType(Material.AIR, false);
            }
        }
        group.placedBarriers.clear();

        // 3) 解绑
        group.members.clear();
        group = null;
        return true;
    }

    boolean resetToBlack() {
        if (group == null)
            return false;
        // 停止任何播放/预加载，避免下一 tick 又把内容刷回来
        stopPlayback();
        // 立即发布一帧纯黑并发送给在范围内的玩家
        initBlackFrame();
        return true;
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
