package me.example.mapframeplayer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
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

class BindManager {
    private final boolean DEBUG = true;

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

    BindManager(JavaPlugin plugin, PlayerManager playerManager) {
        this.plugin = plugin;
        this.playerManager = playerManager;
        this.frameSourceLoader = new FrameSourceLoader(plugin);
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
            feedback.sendMessage("[mplay] Name is empty after sanitizing.");
            return;
        }
        if (!safeName.equals(name)) {
            feedback.sendMessage("[mplay] Name adjusted to " + safeName + " for safety.");
        }

        File framesRoot = new File(plugin.getDataFolder(), "frames");
        if (!framesRoot.exists() && !framesRoot.mkdirs()) {
            feedback.sendMessage("[mplay] Failed to create frames directory.");
            return;
        }
        File targetDir = new File(framesRoot, safeName);
        if (!targetDir.exists() && !targetDir.mkdirs()) {
            feedback.sendMessage("[mplay] Failed to create directory: " + targetDir.getAbsolutePath());
            return;
        }

        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                downloadToFolder(url, targetDir, feedback);
            } catch (IOException e) {
                feedback.sendMessage("[mplay] Download failed: " + e.getMessage());
            }
        });
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
        feedback.sendMessage("[mplay] Starting download...");
        java.net.URL remote = new java.net.URL(url);
        java.net.URLConnection conn = remote.openConnection();
        conn.setRequestProperty("User-Agent", "MapFramePlayer/1.0");
        String path = remote.getPath();
        String ext = determineExtension(path, conn.getContentType());
        File targetFile = new File(targetDir, "default" + ext);

        try (InputStream in = conn.getInputStream();
             java.io.FileOutputStream out = new java.io.FileOutputStream(targetFile)) {
            byte[] buf = new byte[8192];
            int read;
            long total = 0;
            while ((read = in.read(buf)) != -1) {
                out.write(buf, 0, read);
                total += read;
            }
            feedback.sendMessage("[mplay] Downloaded " + humanReadableBytes(total) + " to " + targetFile.getName());
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
        private String sourceLabel = "";

        private final ConcurrentLinkedQueue<byte[]> buffer = new ConcurrentLinkedQueue<>();
        private volatile boolean preloadRunning = false;
        private BukkitTask preloadTask = null;
        private BukkitTask ffmpegTask = null;
        private Process ffmpegProc = null;

        private boolean videoMode = false;
        private File videoFile = null;

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
            if (!videoMode && frames.isEmpty())
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

            if (tick >= nextFrameTick) {
                boolean ready = !buffer.isEmpty();
                if (bufferTarget > 0 && buffer.size() < bufferTarget)
                    ready = false;

                if (ready && !hasAnyPending()) {
                    byte[] linear = buffer.poll();
                    if (linear != null) {
                        publishFrame(linear);

                        if (!videoMode) {
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

        void startPlayback(int tpf, boolean loop, int bufferTarget, int warmupTicks) {
            if (group.members.isEmpty())
                return;

            for (Binding b : group.members) {
                b.hasPendingFrame = false;
                b.scheduledSendTick = -1L;
                b.renderer.clearStagedOnly();
                b.renderer.resetSeen();
            }

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
                    + " tpf=" + this.ticksPerFrame
                    + " loop=" + this.loop
                    + " warmupTicks=" + this.warmupTicks);
        }

        void stopPlayback() {
            stopPreloader();
            buffer.clear();

            for (Binding b : group.members) {
                b.hasPendingFrame = false;
                b.scheduledSendTick = -1L;
                b.renderer.clearStagedOnly();
                b.renderer.resetSeen();
            }

            frames = Collections.emptyList();
            frameIndex = 0;
            ticksPerFrame = 1;
            nextFrameTick = Long.MAX_VALUE;
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
            String framesInfo = videoMode
                    ? ("<video:" + (videoFile != null ? videoFile.getName() : "?") + ">")
                    : String.valueOf(frames.size());
            String playback = videoMode
                    ? ""
                    : (frames.isEmpty() ? "" : (" idx=" + frameIndex + "/" + Math.max(0, frames.size() - 1)));
            return "binding=" + group.members.size() + " maps layout=" + group.cols + "x" + group.rows
                    + " frames=" + framesInfo
                    + (ticksPerFrame == -1 ? " tpf=-1" : " tpf=" + ticksPerFrame)
                    + " loop=" + loop
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

        private void startPreloaderAsync() {
            stopPreloader();
            preloadRunning = true;

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
                            byte[] linear = frameSourceLoader.readFrameLinear(f, expectedWidth(), expectedHeight(), lut);
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
        }

        private void runFfmpegOnce() {
            int tpfLocal = (this.ticksPerFrame <= 0) ? 1 : this.ticksPerFrame;
            double fps = 20.0 / tpfLocal;

            final int W = expectedWidth();
            final int H = expectedHeight();
            final int RGB_BYTES = W * H * 3;

            List<String> cmd = Arrays.asList(
                    "ffmpeg",
                    "-hide_banner", "-loglevel", "error", "-nostdin",
                    "-re",
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
                    if (off < RGB_BYTES)
                        break;
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

        private boolean hasAnyPending() {
            for (Binding b : group.members)
                if (b.hasPendingFrame)
                    return true;
            return false;
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
