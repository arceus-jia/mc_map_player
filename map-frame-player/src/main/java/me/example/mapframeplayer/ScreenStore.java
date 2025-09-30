package me.example.mapframeplayer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.io.File;
import java.util.List;

class ScreenStore {
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final Type listType = new TypeToken<List<PersistedScreen>>(){}.getType();

    List<PersistedScreen> read(File file) throws IOException {
        try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
            return gson.fromJson(reader, listType);
        }
    }

    void write(File file, List<PersistedScreen> screens) throws IOException {
        String json = gson.toJson(screens, listType);
        Files.writeString(file.toPath(), json, StandardCharsets.UTF_8);
    }

    static class PersistedScreen {
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
        Boolean resumeEnabled;
        String resumeMode;
        String resumeFolder;
        Integer resumeTicksPerFrame;
        Boolean resumeLoop;
        Integer resumeWarmupTicks;
        String resumeLiveSource;
        Integer resumeBufferFrames;
    }

    static class BlockPos {
        int x;
        int y;
        int z;
    }

    static class Vec3 {
        double x;
        double y;
        double z;
    }

}
