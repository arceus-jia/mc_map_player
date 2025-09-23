package me.example.mapframeplayer;

import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.entity.Player;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

class PlayerManager {
    Map<World, List<Player>> snapshotOnlineByWorld() {
        Map<World, List<Player>> m = new IdentityHashMap<>();
        for (Player p : Bukkit.getOnlinePlayers()) {
            m.computeIfAbsent(p.getWorld(), w -> new ArrayList<>()).add(p);
        }
        return m;
    }
}
