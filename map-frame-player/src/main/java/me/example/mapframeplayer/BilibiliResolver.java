package me.example.mapframeplayer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

class BilibiliResolver {
    String resolveM3U8(String roomId) throws IOException {
        String api = "https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id=" + roomId
                + "&protocol=0,1&format=0,1,2&codec=0,1&qn=0&platform=web&ptype=8";
        HttpURLConnection conn = (HttpURLConnection) new URL(api).openConnection();
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(7000);
        conn.setRequestProperty("User-Agent", "Mozilla/5.0 (MapFramePlayer)");
        conn.setRequestProperty("Referer", "https://live.bilibili.com/" + roomId);

        int http = conn.getResponseCode();
        if (http != HttpURLConnection.HTTP_OK)
            throw new IOException("HTTP " + http + " while requesting bilibili API");

        String body;
        try (InputStream in = conn.getInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) != -1)
                out.write(buf, 0, len);
            body = out.toString(StandardCharsets.UTF_8.name());
        }

        JsonObject root = JsonParser.parseString(body).getAsJsonObject();
        if (!root.has("code") || root.get("code").getAsInt() != 0)
            throw new IOException("API response error: " + root.get("code"));
        JsonObject data = root.getAsJsonObject("data");
        if (data == null)
            throw new IOException("Missing data field");
        JsonObject playurlInfo = data.getAsJsonObject("playurl_info");
        if (playurlInfo == null)
            throw new IOException("Missing playurl_info field");
        JsonObject playurl = playurlInfo.getAsJsonObject("playurl");
        if (playurl == null)
            throw new IOException("Missing playurl field");

        JsonArray streams = playurl.getAsJsonArray("stream");
        if (streams == null || streams.size() == 0)
            throw new IOException("No available stream data");

        for (JsonElement streamElem : streams) {
            JsonObject stream = streamElem.getAsJsonObject();
            JsonArray formats = stream.getAsJsonArray("format");
            if (formats == null)
                continue;
            for (JsonElement formatElem : formats) {
                JsonObject format = formatElem.getAsJsonObject();
                JsonArray codecs = format.getAsJsonArray("codec");
                if (codecs == null)
                    continue;
                for (JsonElement codecElem : codecs) {
                    JsonObject codec = codecElem.getAsJsonObject();
                    String baseUrl = codec.has("base_url") ? codec.get("base_url").getAsString() : null;
                    JsonArray urlInfos = codec.getAsJsonArray("url_info");
                    if (baseUrl == null || urlInfos == null || urlInfos.size() == 0)
                        continue;
                    JsonObject info = urlInfos.get(0).getAsJsonObject();
                    String host = info.get("host").getAsString();
                    String extra = info.has("extra") ? info.get("extra").getAsString() : "";
                    if (host != null)
                        return host + baseUrl + extra;
                }
            }
        }

        throw new IOException("Unable to locate a playable URL");
    }
}

