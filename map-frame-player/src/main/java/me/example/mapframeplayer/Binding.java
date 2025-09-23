package me.example.mapframeplayer;

import org.bukkit.World;
import org.bukkit.map.MapView;

class Binding {
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
