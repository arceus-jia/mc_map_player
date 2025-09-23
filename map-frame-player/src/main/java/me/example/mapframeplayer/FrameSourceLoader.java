package me.example.mapframeplayer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.bukkit.plugin.java.JavaPlugin;

import javax.imageio.ImageIO;
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
import java.util.List;
import java.util.Locale;

class FrameSourceLoader {
    private final JavaPlugin plugin;
    FrameSourceLoader(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    FrameLoadResult loadFromFolder(File dataFolder, String folderPath, int expectedWidth, int expectedHeight, byte[] lut)
            throws IOException {
        File folder = new File(dataFolder, "frames/" + folderPath);

        plugin.getLogger().info("Trying to load frames from: " + folder.getAbsolutePath());
        if (!folder.exists())
            throw new IOException("Folder does not exist: " + folder.getAbsolutePath());
        if (!folder.isDirectory())
            throw new IOException("Path is not a directory: " + folder.getAbsolutePath());

        File[] files = folder.listFiles((dir, name) -> {
            String n = name.toLowerCase(Locale.ROOT);
            return n.endsWith(".json") || n.endsWith(".smrf") || n.endsWith(".png") || n.endsWith(".jpg")
                    || n.endsWith(".jpeg") || isVideoName(n);
        });
        if (files == null || files.length == 0)
            throw new IOException("No frame files found in: " + folder.getAbsolutePath());

        if (files.length == 1 && isVideoFile(files[0])) {
            plugin.getLogger().info("[mplay] video mode: " + files[0].getName());
            FrameLoadResult result = new FrameLoadResult();
            result.videoMode = true;
            result.videoFile = files[0];
            result.frameFiles = Collections.emptyList();
            result.frameCount = 1;
            result.sourceLabel = folderPath + "/" + files[0].getName();
            return result;
        }

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

        if (frameFiles.length == 0)
            throw new IOException("No frame files found in (non-video): " + folder.getAbsolutePath());

        plugin.getLogger().info("Found " + frameFiles.length + " frame(s). First = " + frameFiles[0].getName());
        File first = frameFiles[0];
        if (isJsonFile(first)) {
            int[] wh = peekJsonSize(first);
            if (wh[0] != expectedWidth || wh[1] != expectedHeight)
                throw new IOException("JSON frame size mismatch.");
        } else if (isSmrfFile(first)) {
            long len = first.length();
            if (len != (long) expectedWidth * (long) expectedHeight)
                throw new IOException("SMRF length mismatch.");
        } else if (isImageFile(first)) {
            BufferedImage img0 = ImageIO.read(first);
            if (img0 == null)
                throw new IOException("Image read failed: " + first.getName());
        } else {
            throw new IOException("Unknown frame type: " + first.getName());
        }

        FrameLoadResult result = new FrameLoadResult();
        result.videoMode = false;
        result.videoFile = null;
        result.frameFiles = Arrays.asList(frameFiles);
        result.frameCount = frameFiles.length;
        result.sourceLabel = folderPath;
        plugin.getLogger().info("Frames loaded successfully: " + result.frameCount);
        return result;
    }

    byte[] readFrameLinear(File file, int expectedWidth, int expectedHeight, byte[] lut) throws IOException {
        if (isJsonFile(file))
            return readJsonLinear(file, expectedWidth, expectedHeight, lut);
        if (isSmrfFile(file))
            return readSmrfLinear(file, expectedWidth * expectedHeight);
        if (isImageFile(file))
            return readImageLinear(file, expectedWidth, expectedHeight, lut);
        throw new IOException("unsupported frame type: " + file.getName());
    }

    byte[] rgb24ToPalette(byte[] rgb, int width, int height, byte[] lut) throws IOException {
        if (lut == null)
            throw new IOException("LUT not loaded");
        byte[] out = new byte[width * height];
        int p = 0;
        for (int i = 0; i < out.length; i++) {
            int r = rgb[p++] & 0xFF;
            int g = rgb[p++] & 0xFF;
            int b = rgb[p++] & 0xFF;
            int key = (r << 16) | (g << 8) | b;
            out[i] = lut[key];
        }
        return out;
    }

    private byte[] readJsonLinear(File file, int expectedWidth, int expectedHeight, byte[] lut) throws IOException {
        try (Reader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
            JsonArray arr = JsonParser.parseReader(r).getAsJsonArray();
            int h = arr.size();
            if (h == 0)
                throw new IOException("empty array");
            JsonElement firstElem = arr.get(0);
            if (!firstElem.isJsonArray())
                throw new IOException("row[0] is not an array");
            JsonArray row0 = firstElem.getAsJsonArray();
            int w = row0.size();
            if (w != expectedWidth || h != expectedHeight)
                throw new IOException("frame size mismatch: " + w + "x" + h);

            byte[] out = new byte[w * h];
            int pos = 0;
            for (int y = 0; y < h; y++) {
                JsonElement rowElem = arr.get(y);
                if (!rowElem.isJsonArray())
                    throw new IOException("row@" + y + " is not an array");
                JsonArray row = rowElem.getAsJsonArray();
                if (row.size() != w)
                    throw new IOException("bad row width @y=" + y);
                for (int x = 0; x < w; x++) {
                    out[pos++] = resolvePixel(row.get(x), lut);
                }
            }
            return out;
        } catch (IllegalStateException ex) {
            throw new IOException("invalid json frame: " + ex.getMessage(), ex);
        }
    }

    private byte[] readSmrfLinear(File file, int expectedBytes) throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
            byte[] buf = in.readNBytes(expectedBytes);
            if (buf.length != expectedBytes)
                throw new IOException("smrf length mismatch: got=" + buf.length + " need=" + expectedBytes);
            return buf;
        }
    }

    private byte[] readImageLinear(File file, int expectedWidth, int expectedHeight, byte[] lut) throws IOException {
        if (lut == null)
            throw new IOException("LUT not loaded; put colormap.lut under plugins/MapFramePlayer/");
        BufferedImage img = ImageIO.read(file);
        if (img == null)
            throw new IOException("ImageIO.read returned null for " + file.getName());
        BufferedImage scaled = (img.getWidth() == expectedWidth && img.getHeight() == expectedHeight)
                ? img
                : resizeImage(img, expectedWidth, expectedHeight);

        int[] rgb = scaled.getRGB(0, 0, expectedWidth, expectedHeight, null, 0, expectedWidth);
        byte[] out = new byte[expectedWidth * expectedHeight];
        for (int i = 0; i < rgb.length; i++) {
            int p = rgb[i];
            int r = (p >>> 16) & 0xFF;
            int g = (p >>> 8) & 0xFF;
            int b = p & 0xFF;
            int key = (r << 16) | (g << 8) | b;
            out[i] = lut[key];
        }
        return out;
    }

    private byte resolvePixel(JsonElement elem, byte[] lut) throws IOException {
        if (elem == null || elem.isJsonNull())
            return 0;
        if (elem.isJsonPrimitive())
            return primitiveToByte(elem.getAsJsonPrimitive());
        if (elem.isJsonArray())
            return arrayToByte(elem.getAsJsonArray(), lut);
        if (elem.isJsonObject())
            return objectToByte(elem.getAsJsonObject(), lut);
        throw new IOException("unsupported json pixel element: " + elem);
    }

    private byte primitiveToByte(JsonPrimitive prim) throws IOException {
        if (prim.isNumber())
            return (byte) (prim.getAsInt() & 0xFF);
        if (prim.isString()) {
            try {
                return (byte) (Integer.parseInt(prim.getAsString().trim()) & 0xFF);
            } catch (NumberFormatException ex) {
                throw new IOException("invalid numeric string: " + prim.getAsString(), ex);
            }
        }
        throw new IOException("unsupported primitive: " + prim);
    }

    private byte arrayToByte(JsonArray arr, byte[] lut) throws IOException {
        if (arr.size() == 0)
            return 0;
        if (arr.size() == 1)
            return resolvePixel(arr.get(0), lut);
        if (arr.size() >= 3)
            return rgbToPalette(arr, lut);
        return resolvePixel(arr.get(0), lut);
    }

    private byte objectToByte(JsonObject obj, byte[] lut) throws IOException {
        if (obj.has("index"))
            return resolvePixel(obj.get("index"), lut);
        if (obj.has("value"))
            return resolvePixel(obj.get("value"), lut);
        if (obj.has("palette"))
            return resolvePixel(obj.get("palette"), lut);
        if (obj.has("rgb")) {
            JsonElement rgb = obj.get("rgb");
            if (rgb.isJsonArray())
                return rgbToPalette(rgb.getAsJsonArray(), lut);
        }
        if (obj.has("r") && obj.has("g") && obj.has("b")) {
            return rgbToPalette(
                    channelValue(obj.get("r")),
                    channelValue(obj.get("g")),
                    channelValue(obj.get("b")),
                    lut);
        }
        throw new IOException("unsupported pixel object keys: " + obj.keySet());
    }

    private byte rgbToPalette(JsonArray arr, byte[] lut) throws IOException {
        if (arr.size() < 3)
            throw new IOException("rgb array must have >=3 elements");
        int r = channelValue(arr.get(0));
        int g = channelValue(arr.get(1));
        int b = channelValue(arr.get(2));
        return rgbToPalette(r, g, b, lut);
    }

    private byte rgbToPalette(int r, int g, int b, byte[] lut) throws IOException {
        if (lut == null)
            throw new IOException("LUT not loaded; cannot convert RGB");
        int key = ((r & 0xFF) << 16) | ((g & 0xFF) << 8) | (b & 0xFF);
        return lut[key];
    }

    private int channelValue(JsonElement elem) throws IOException {
        if (elem.isJsonPrimitive()) {
            JsonPrimitive prim = elem.getAsJsonPrimitive();
            if (prim.isNumber())
                return clampChannel(prim.getAsInt());
            if (prim.isString()) {
                try {
                    return clampChannel(Integer.parseInt(prim.getAsString().trim()));
                } catch (NumberFormatException ex) {
                    throw new IOException("invalid channel string: " + prim.getAsString(), ex);
                }
            }
        }
        throw new IOException("invalid rgb channel element: " + elem);
    }

    private int clampChannel(int v) {
        if (v < 0)
            return 0;
        if (v > 255)
            return 255;
        return v;
    }

    private int[] peekJsonSize(File file) throws IOException {
        try (Reader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
            JsonArray arr = JsonParser.parseReader(r).getAsJsonArray();
            int h = arr.size();
            if (h == 0)
                throw new IOException("empty array");
            JsonElement first = arr.get(0);
            if (!first.isJsonArray())
                throw new IOException("row[0] is not an array");
            int w = first.getAsJsonArray().size();
            return new int[] { w, h };
        }
    }

    private boolean isJsonFile(File f) {
        String n = f.getName().toLowerCase(Locale.ROOT);
        return n.endsWith(".json");
    }

    private boolean isSmrfFile(File f) {
        String n = f.getName().toLowerCase(Locale.ROOT);
        return n.endsWith(".smrf");
    }

    private boolean isImageFile(File f) {
        String n = f.getName().toLowerCase(Locale.ROOT);
        return n.endsWith(".png") || n.endsWith(".jpg") || n.endsWith(".jpeg");
    }

    private boolean isVideoFile(File f) {
        return isVideoName(f.getName().toLowerCase(Locale.ROOT));
    }

    private boolean isVideoName(String n) {
        return n.endsWith(".mp4") || n.endsWith(".mov") || n.endsWith(".m4v")
                || n.endsWith(".avi") || n.endsWith(".webm") || n.endsWith(".wmv")
                || n.endsWith(".ts") || n.endsWith(".m3u8") || n.endsWith(".gif");
    }

    private BufferedImage resizeImage(BufferedImage src, int dstW, int dstH) {
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

    static class FrameLoadResult {
        boolean videoMode;
        File videoFile;
        List<File> frameFiles;
        int frameCount;
        String sourceLabel;
    }
}
