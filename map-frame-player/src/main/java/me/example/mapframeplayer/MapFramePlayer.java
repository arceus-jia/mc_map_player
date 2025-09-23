package me.example.mapframeplayer;

import org.bukkit.ChatColor;
import org.bukkit.Location;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
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
                case "create": {
                    // /mplay create <cols> <rows> [radius] [x y z]
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

                    // 限制最大 16x9
                    if (cols <= 0 || rows <= 0 || cols * rows > 16 * 9) {
                        s.sendMessage(color("&cScreen too large. Max size is 16x9. You asked: " + cols + "x" + rows));
                        return true;
                    }

                    int radius = 0;
                    Location anchor = null;

                    if (a.length == 4) {
                        // 只有 radius
                        radius = Integer.parseInt(a[3]);
                    } else if (a.length == 6) {
                        // 只有 xyz（radius=0）
                        int x = Integer.parseInt(a[3]);
                        int y = Integer.parseInt(a[4]);
                        int z = Integer.parseInt(a[5]);
                        anchor = new Location(ps.getWorld(), x, y, z);
                    } else if (a.length == 7) {
                        // radius + xyz
                        radius = Integer.parseInt(a[3]);
                        int x = Integer.parseInt(a[4]);
                        int y = Integer.parseInt(a[5]);
                        int z = Integer.parseInt(a[6]);
                        anchor = new Location(ps.getWorld(), x, y, z);
                    }

                    boolean ok = binds.createAndBindFloatingAtPlayer(ps, cols, rows, radius, /* forward */2, anchor);
                    if (!ok) {
                        s.sendMessage(color("&cCreate failed. Make sure there is free space."));
                    } else {
                        s.sendMessage(color("&aCreated a " + cols + "x" + rows + " screen. radius=" + radius
                                + (anchor != null
                                        ? (" at " + anchor.getBlockX() + "," + anchor.getBlockY() + ","
                                                + anchor.getBlockZ())
                                        : " (in front of you)")));
                    }
                    return true;
                }
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

                    Location center = null;
                    if (s instanceof Player) {
                        Player ps = (Player) s;
                        if (ps.getWorld().getName().equals(worldName))
                            center = ps.getLocation();
                    }
                    boolean ok = binds.bind(ids, worldName, cols, rows, radius, center);

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
                    boolean loop = (a.length >= 4) ? Boolean.parseBoolean(a[3]) : false;
                    int warmup = (a.length >= 5) ? Integer.parseInt(a[4]) : 0; // 新：warmupTicks

                    int loaded = binds.loadFramesFromFolder(folder);
                    if (loaded <= 0) {
                        s.sendMessage(color("&cNo frames loaded. Check folder & size. " + folder));
                        return true;
                    }
                    binds.startPlayback(tpf, loop, /* bufferTarget */ 0, /* warmupTicks */ warmup);
                    s.sendMessage(color("&aPlaying " + loaded + " frames at " + tpf + " tpf; loop=" + loop
                            + (warmup > 0 ? (" warmupTicks=" + warmup) : "")));
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
                case "clear": {
                    boolean ok = binds.clearScreen();
                    if (ok)
                        s.sendMessage(color("&aCleared current screen and invisible backing."));
                    else
                        s.sendMessage(color("&cNo active screen to clear."));
                    return true;
                }
                case "reset": {
                    boolean ok = binds.resetToBlack();
                    if (ok)
                        s.sendMessage(color("&aScreen reset to black."));
                    else
                        s.sendMessage(color("&cNo active binding to reset."));
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
        s.sendMessage(color("&f/mplay play <folder> <ticksPerFrame> [loop] [warmupTicks]"));
        s.sendMessage(color("&7warmupTicks: 开播前延迟的 tick 数（20 tick = 1 秒）"));

        s.sendMessage(color("&f/mplay stop"));
        s.sendMessage(color("&f/mplay status"));
        s.sendMessage(color(
                "&7Frames: .json (HxW int), .smrf (raw W*H bytes), .png/.jpg (RGB via LUT), or a single video file (ffmpeg)."));

        s.sendMessage(color("&f/mplay create <cols> <rows> [radius] [x y z]"));
        s.sendMessage(color("&f/mplay clear  &7— remove the last created screen (frames + barrier)"));
        s.sendMessage(color("&f/mplay reset  &7— stop playback and show a black screen"));

    }

    private static String color(String s) {
        return ChatColor.translateAlternateColorCodes('&', s);
    }
}
