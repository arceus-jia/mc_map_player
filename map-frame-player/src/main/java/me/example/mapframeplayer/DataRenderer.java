package me.example.mapframeplayer;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.map.MapCanvas;
import org.bukkit.map.MapRenderer;
import org.bukkit.map.MapView;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class DataRenderer extends MapRenderer {
    private final byte[] current = new byte[128 * 128];
    private long currentEpoch = 0L;

    private final byte[] staged = new byte[128 * 128];
    private long stagedEpoch = 0L;
    private boolean hasStaged = false;

    private final Map<UUID, Long> seen = new HashMap<>();

    private World rangeWorld;
    private Location rangeCenter;
    private int rangeMaxDist;

    synchronized void setRange(World w, Location center, int maxDist) {
        this.rangeWorld = w;
        this.rangeCenter = (center == null ? null : center.clone());
        this.rangeMaxDist = maxDist;
    }

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
        // —— 距离拦截：不在范围内就直接 return，不要更新 seen ——
        if (rangeMaxDist > 0) {
            if (player.getWorld() != rangeWorld)
                return;
            Location lp = player.getLocation();
            double dx = lp.getX() - rangeCenter.getX();
            double dy = lp.getY() - rangeCenter.getY();
            double dz = lp.getZ() - rangeCenter.getZ();
            if ((dx * dx + dy * dy + dz * dz) > (double) rangeMaxDist * (double) rangeMaxDist) {
                return;
            }
        }

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
