package me.example.mapframeplayer;

import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerChangedWorldEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.event.player.PlayerRespawnEvent;
import org.bukkit.Bukkit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.List;
import java.util.Locale;

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

    private final PlayerManager playerMgr = new PlayerManager();
    private final BindManager binds = new BindManager(this, playerMgr);

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
        // Ensure config defaults exist
        try {
            getConfig().addDefault("debug", false);
            getConfig().options().copyDefaults(true);
            saveConfig();
        } catch (Throwable ignore) {
        }

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
        binds.loadPersistedScreens();

        getServer().getPluginManager().registerEvents(new PlayerEvents(), this);
    }

    @Override
    public void onDisable() {
        binds.stop();
        getLogger().info("MapFramePlayer disabled.");
    }

    private final class PlayerEvents implements Listener {
        @EventHandler
        public void handleJoin(PlayerJoinEvent event) {
            binds.refreshViewer(event.getPlayer());
        }

        @EventHandler
        public void handleRespawn(PlayerRespawnEvent event) {
            Player player = event.getPlayer();
            Bukkit.getScheduler().runTask(MapFramePlayer.this, () -> binds.refreshViewer(player));
        }

        @EventHandler
        public void handleWorldChange(PlayerChangedWorldEvent event) {
            binds.refreshViewer(event.getPlayer());
        }

        @EventHandler
        public void handleQuit(PlayerQuitEvent event) {
            binds.forgetViewer(event.getPlayer().getUniqueId());
        }
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
                case "create": {
                    if (!(s instanceof Player)) {
                        s.sendMessage(color("&cThis command must be run by a player."));
                        return true;
                    }
                    if (a.length < 3 || a.length > 7) {
                        s.sendMessage(color("&f/mplay create <cols> <rows> [radius] [x y z]"));
                        s.sendMessage(color("&7radius默认0=不限；xyz是屏幕平面左下角的方块坐标（可选）。"));
                        return true;
                    }

                    Player ps = (Player) s;
                    int cols = Integer.parseInt(a[1]);
                    int rows = Integer.parseInt(a[2]);

                    if (cols <= 0 || rows <= 0 || cols * rows > 16 * 9) {
                        s.sendMessage(color("&cScreen too large. Max size is 16x9. You asked: " + cols + "x" + rows));
                        return true;
                    }

                    int radius = 0;
                    Location anchor = null;

                    if (a.length == 4) {
                        radius = Integer.parseInt(a[3]);
                    } else if (a.length == 6) {
                        int x = Integer.parseInt(a[3]);
                        int y = Integer.parseInt(a[4]);
                        int z = Integer.parseInt(a[5]);
                        anchor = new Location(ps.getWorld(), x, y, z);
                    } else if (a.length == 7) {
                        radius = Integer.parseInt(a[3]);
                        int x = Integer.parseInt(a[4]);
                        int y = Integer.parseInt(a[5]);
                        int z = Integer.parseInt(a[6]);
                        anchor = new Location(ps.getWorld(), x, y, z);
                    }

                    Integer screenId = binds.createAndBindFloatingAtPlayer(ps, cols, rows, radius, /* forward */2,
                            anchor);
                    if (screenId == null) {
                        s.sendMessage(color("&cCreate failed. Make sure there is free space."));
                    } else {
                        s.sendMessage(color("&aCreated screen #" + screenId + " (" + cols + "x" + rows + ") radius="
                                + radius + (anchor != null
                                        ? (" at " + anchor.getBlockX() + "," + anchor.getBlockY() + ","
                                                + anchor.getBlockZ())
                                        : " (in front of you)")));
                    }
                    return true;
                }
                case "set": {
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
                    int radius = (a.length == 6) ? Integer.parseInt(a[5]) : 0;

                    if (cols <= 0 || rows <= 0) {
                        s.sendMessage(color("&ccols/rows must > 0"));
                        return true;
                    }
                    if (ids.length != cols * rows) {
                        s.sendMessage(color(
                                "&cmapId count mismatch cols*rows: " + ids.length + " != " + (cols * rows)));
                        return true;
                    }

                    Location center = null;
                    if (s instanceof Player) {
                        Player ps = (Player) s;
                        if (ps.getWorld().getName().equals(worldName))
                            center = ps.getLocation();
                    }
                    Integer screenId = binds.bind(ids, worldName, cols, rows, radius, center);

                    if (screenId == null) {
                        s.sendMessage(color("&cBind failed. Check world and map ids."));
                    } else {
                        s.sendMessage(color("&aBound screen #" + screenId + " layout=" + cols + "x" + rows
                                + " radius=" + radius));
                    }
                    return true;
                }
                case "play": {
                    TargetParseResult target = parseOptionalScreenId(a, 1);
                    int idx = target.nextIndex;
                    Integer screenId = target.screenId != null ? target.screenId : binds.lastActiveId();

                    if (idx >= a.length) {
                        s.sendMessage(color("&f/mplay play [id <screenId>] <folder> [tpf] [loop] [warmupTicks]"));
                        return true;
                    }

                    if (screenId == null) {
                        s.sendMessage(color("&cNo screen selected. Use id or create one first."));
                        return true;
                    }

                    String folder = a[idx++];
                    int tpf = (idx < a.length) ? Integer.parseInt(a[idx++]) : 1;
                    boolean loop = (idx < a.length) ? Boolean.parseBoolean(a[idx++]) : false;
                    int warmup = (idx < a.length) ? Integer.parseInt(a[idx++]) : 0;

                    if (!binds.isLutLoaded()) {
                        s.sendMessage(color("&cLUT 未加载：请将 colormap.lut 放到 plugins/MapFramePlayer/ 下，再重试。"));
                        return true;
                    }

                    int loaded = binds.loadFramesFromFolder(screenId, folder);
                    if (loaded <= 0) {
                        s.sendMessage(color("&cNo frames loaded for screen #" + screenId + ". Check folder & size."));
                        return true;
                    }
                    // 若为视频模式，需要 ffmpeg
                    if (binds.willUseFfmpeg(screenId) && !binds.isFfmpegAvailable()) {
                        s.sendMessage(color("&c未检测到 ffmpeg，请确认已安装并加入 PATH。"));
                        return true;
                    }
                    binds.startPlayback(screenId, tpf, loop, 0, warmup);
                    s.sendMessage(color("&aScreen #" + screenId + " playing " + loaded + " frame(s) at " + tpf
                            + " tpf; loop=" + loop
                            + (warmup > 0 ? (" warmupTicks=" + warmup) : "")));
                    return true;
                }

                case "stop": {
                    if (a.length >= 2 && a[1].equalsIgnoreCase("all")) {
                        int stopped = binds.stopAllPlayback();
                        s.sendMessage(color("&aStopped playback on " + stopped + " screen(s)."));
                        return true;
                    }
                    TargetParseResult target = parseOptionalScreenId(a, 1);
                    Integer screenId = target.screenId != null ? target.screenId : binds.lastActiveId();
                    if (screenId == null) {
                        s.sendMessage(color("&cNo screen selected to stop."));
                        return true;
                    }
                    boolean ok = binds.stopPlayback(screenId);
                    if (ok)
                        s.sendMessage(color("&aStopped playback on screen #" + screenId + "."));
                    else
                        s.sendMessage(color("&cScreen #" + screenId + " not found."));
                    return true;
                }
                case "status": {
                    if (a.length >= 2 && a[1].equalsIgnoreCase("all")) {
                        for (String line : binds.listStatus())
                            s.sendMessage(color("&f" + line));
                        return true;
                    }
                    TargetParseResult target = parseOptionalScreenId(a, 1);
                    Integer screenId = target.screenId != null ? target.screenId : binds.lastActiveId();
                    if (screenId == null) {
                        s.sendMessage(color("&cNo screen selected."));
                        return true;
                    }
                    s.sendMessage(color("&f" + binds.statusString(screenId)));
                    return true;
                }
                case "list": {
                    if (!binds.hasScreens()) {
                        s.sendMessage(color("&7No screens registered."));
                        return true;
                    }
                    for (String line : binds.listStatus())
                        s.sendMessage(color("&f" + line));
                    return true;
                }
                case "media": {
                    if (a.length == 1) {
                        s.sendMessage(color("&eMedia 子命令:"));
                        s.sendMessage(color("&f/mplay media list"));
                        s.sendMessage(color("&f/mplay media download <name> <url>"));
                        s.sendMessage(color("&f/mplay media delete <name>"));
                        s.sendMessage(color("&f/mplay media rename <old> <new>"));
                        s.sendMessage(color("&f/mplay media cancel"));
                        s.sendMessage(color("&f/mplay media transcode <name> [fps] [quality]"));
                        return true;
                    }
                    String sub2 = a[1].toLowerCase(Locale.ROOT);
                    switch (sub2) {
                        case "help": {
                            s.sendMessage(color("&eMedia 子命令:"));
                            s.sendMessage(color("&f/mplay media list"));
                            s.sendMessage(color("&f/mplay media download <name> <url>"));
                            s.sendMessage(color("&f/mplay media delete <name>"));
                            s.sendMessage(color("&f/mplay media rename <old> <new>"));
                            s.sendMessage(color("&f/mplay media cancel"));
                            s.sendMessage(color("&f/mplay media transcode <name> [fps] [quality]"));
                            return true;
                        }
                        case "list": {
                            List<String> entries = binds.listMediaEntries();
                            if (entries.isEmpty()) {
                                s.sendMessage(color("&7No media found under frames/"));
                            } else {
                                s.sendMessage(color("&eAvailable media:"));
                                for (String entry : entries)
                                    s.sendMessage(color("&f" + entry));
                            }
                            return true;
                        }
                        case "download": {
                            if (a.length < 4) {
                                s.sendMessage(color("&f/mplay media download <name> <url>"));
                                return true;
                            }
                            String name = a[2];
                            String url = a[3];
                            binds.downloadMedia(name, url, s);
                            return true;
                        }
                        case "delete": {
                            if (a.length < 3) {
                                s.sendMessage(color("&f/mplay media delete <name>"));
                                return true;
                            }
                            binds.deleteMedia(a[2], s);
                            return true;
                        }
                        case "rename": {
                            if (a.length < 4) {
                                s.sendMessage(color("&f/mplay media rename <old> <new>"));
                                return true;
                            }
                            binds.renameMedia(a[2], a[3], s);
                            return true;
                        }
                        case "transcode": {
                            if (a.length < 3) {
                                s.sendMessage(color("&f/mplay media transcode <name> [fps] [quality]"));
                                s.sendMessage(color("&7quality 示例: 480p / 720p / 1080p 或 1280x720；不放大小于目标的视频"));
                                return true;
                            }
                            String name = a[2];
                            Integer fps = null;
                            Integer height = null;
                            int idx = 3;
                            if (idx < a.length && isNumeric(a[idx])) {
                                fps = Integer.parseInt(a[idx++]);
                            }
                            if (idx < a.length) {
                                height = parseQualityHeight(a[idx]);
                            }
                            binds.transcodeMedia(name, fps, height, s);
                            return true;
                        }
                        case "cancel": {
                            binds.cancelCurrentMediaOperation(s);
                            return true;
                        }
                        default: {
                            s.sendMessage(color("&7未知子命令，使用 &f/mplay media&7 查看帮助"));
                            return true;
                        }
                    }
                }
                case "live": {
                    TargetParseResult target = parseOptionalScreenId(a, 1);
                    int idx = target.nextIndex;
                    Integer screenId = target.screenId != null ? target.screenId : binds.lastActiveId();
                    if (screenId == null) {
                        s.sendMessage(color("&cNo screen selected."));
                        return true;
                    }
                    if (idx >= a.length) {
                        s.sendMessage(color(
                                "&f/mplay live [id <screenId>] <m3u8OrName> [ticksPerFrame] [bufferFrames]"));
                        return true;
                    }
                    String source = a[idx++];
                    Integer tpf = null;
                    Integer bufferFrames = null;
                    if (idx < a.length)
                        tpf = Integer.parseInt(a[idx++]);
                    if (idx < a.length)
                        bufferFrames = Integer.parseInt(a[idx++]);

                    if (!binds.isLutLoaded()) {
                        s.sendMessage(color("&cLUT 未加载：请将 colormap.lut 放到 plugins/MapFramePlayer/ 下，再重试。"));
                        return true;
                    }
                    if (!binds.isFfmpegAvailable()) {
                        s.sendMessage(color("&c未检测到 ffmpeg，请确认已安装并加入 PATH。"));
                        return true;
                    }
                    binds.startLiveStream(screenId, source, tpf != null ? tpf : 0,
                            bufferFrames, s);
                    return true;
                }
                case "bilibili": {
                    if (a.length < 3) {
                        s.sendMessage(color("&f/mplay bilibili <name> <roomId>"));
                        return true;
                    }
                    String name = a[1];
                    String roomId = a[2];
                    binds.fetchBilibiliStream(name, roomId, s);
                    return true;
                }
                case "debug": {
                    if (a.length < 2) {
                        s.sendMessage(color("&f/mplay debug <true|false>"));
                        return true;
                    }
                    boolean val = Boolean.parseBoolean(a[1]);
                    try {
                        getConfig().set("debug", val);
                        saveConfig();
                    } catch (Throwable ignore) {}
                    binds.setDebug(val);
                    s.sendMessage(color("&aDebug 已设置为: " + val));
                    return true;
                }
                case "clear": {
                    if (a.length >= 2 && a[1].equalsIgnoreCase("all")) {
                        int cleared = binds.clearAllScreens();
                        s.sendMessage(cleared > 0
                                ? color("&aCleared " + cleared + " screen(s).")
                                : color("&cNo screens to clear."));
                        return true;
                    }
                    TargetParseResult target = parseOptionalScreenId(a, 1);
                    Integer screenId = target.screenId != null ? target.screenId : binds.lastActiveId();
                    if (screenId == null) {
                        s.sendMessage(color("&cNo screen selected to clear."));
                        return true;
                    }
                    boolean ok = binds.clearScreen(screenId);
                    if (ok)
                        s.sendMessage(color("&aCleared screen #" + screenId + " (frames + barrier)."));
                    else
                        s.sendMessage(color("&cScreen #" + screenId + " not found."));
                    return true;
                }
                case "reset": {
                    TargetParseResult target = parseOptionalScreenId(a, 1);
                    Integer screenId = target.screenId != null ? target.screenId : binds.lastActiveId();
                    if (screenId == null) {
                        s.sendMessage(color("&cNo screen selected to reset."));
                        return true;
                    }
                    boolean ok = binds.resetToBlack(screenId);
                    if (ok)
                        s.sendMessage(color("&aScreen #" + screenId + " reset to black."));
                    else
                        s.sendMessage(color("&cScreen #" + screenId + " not found."));
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
        // 屏幕管理
        s.sendMessage(color("&f/mplay create <cols> <rows> [radius] [x y z] &7# 创建新屏幕"));
        s.sendMessage(color("&f/mplay set <id1,id2,...> <world> <cols> <rows> [radius] &7# 绑定现有地图"));
        s.sendMessage(color("&f/mplay list &7# 列出所有屏幕"));
        s.sendMessage(color("&f/mplay status [id <screenId>|all] &7# 查看状态"));
        s.sendMessage(color("&f/mplay clear [id <screenId>|all] &7# 清理并注销屏幕"));
        s.sendMessage(color("&f/mplay reset [id <screenId>] &7# 重置为黑屏"));

        // 播放控制
        s.sendMessage(color("&f/mplay play [id <screenId>] <folder> [tpf] [loop] [warmupTicks]"));
        s.sendMessage(color("&7tpf: -1=定格, 0=源驱动, >0=固定节奏(20/tpf fps); warmupTicks: 启播延迟"));
        s.sendMessage(color("&f/mplay stop [id <screenId>|all]"));

        // 媒体管理
        s.sendMessage(color("&f/mplay media list"));
        s.sendMessage(color("&f/mplay media download <name> <url>"));
        s.sendMessage(color("&f/mplay media delete <name>"));
        s.sendMessage(color("&f/mplay media rename <old> <new>"));

        // 直播与扩展
        s.sendMessage(color("&f/mplay bilibili <name> <roomId> &7# 解析并保存 m3u8"));
        s.sendMessage(color("&f/mplay live [id <screenId>] <m3u8OrName> [ticksPerFrame] [bufferFrames]"));
        s.sendMessage(color("&f/mplay debug <true|false>"));

        // 素材类型
        s.sendMessage(color(
                "&7Frames: .json (HxW int), .smrf (raw W*H bytes), .png/.jpg (RGB via LUT), video file (ffmpeg)."));

    }

    private static String color(String s) {
        return ChatColor.translateAlternateColorCodes('&', s);
    }

    private TargetParseResult parseOptionalScreenId(String[] args, int start) {
        if (start >= args.length)
            return new TargetParseResult(null, start);

        String token = args[start];
        if ("id".equalsIgnoreCase(token)) {
            if (start + 1 >= args.length)
                throw new NumberFormatException("missing screen id");
            int id = Integer.parseInt(args[start + 1]);
            return new TargetParseResult(id, start + 2);
        }

        if (isNumeric(token)) {
            int id = Integer.parseInt(token);
            return new TargetParseResult(id, start + 1);
        }

        return new TargetParseResult(null, start);
    }

    private boolean isNumeric(String token) {
        try {
            Integer.parseInt(token);
            return true;
        } catch (NumberFormatException ignore) {
            return false;
        }
    }

    private Integer parseQualityHeight(String token) {
        if (token == null) return null;
        String t = token.trim().toLowerCase(Locale.ROOT);
        // forms: 720p, 720, 1280x720, 720px
        if (t.endsWith("px")) t = t.substring(0, t.length() - 2);
        if (t.endsWith("p")) t = t.substring(0, t.length() - 1);
        int x = t.indexOf('x');
        if (x > 0) {
            String h = t.substring(x + 1);
            try { return Integer.parseInt(h); } catch (NumberFormatException ignore) {}
        }
        try { return Integer.parseInt(t); } catch (NumberFormatException ignore) {}
        return null;
    }

    private static class TargetParseResult {
        final Integer screenId;
        final int nextIndex;

        TargetParseResult(Integer screenId, int nextIndex) {
            this.screenId = screenId;
            this.nextIndex = nextIndex;
        }
    }
}
