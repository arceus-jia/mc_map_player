package me.example.mapframeplayer;

import org.bukkit.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class BindingGroup {
    final String worldName;
    final int cols;
    final int rows;
    final int radius;
    List<Binding> members = new ArrayList<>();
    long epochCounter = 0L;
    Location center;
    List<Location> placedBarriers = new ArrayList<>();
    List<UUID> frameUUIDs = new ArrayList<>();

    BindingGroup(String worldName, int cols, int rows, int radius) {
        this.worldName = worldName;
        this.cols = cols;
        this.rows = rows;
        this.radius = radius;
    }
}
