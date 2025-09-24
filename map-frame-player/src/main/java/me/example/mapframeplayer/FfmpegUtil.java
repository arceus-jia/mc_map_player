package me.example.mapframeplayer;

class FfmpegUtil {
    private static volatile Boolean cache = null;
    private static volatile long lastCheck = 0L;

    static boolean isAvailable() {
        long now = System.currentTimeMillis();
        if (cache != null && (now - lastCheck) < 30_000L) return cache;
        boolean ok = false;
        try {
            Process p = new ProcessBuilder("ffmpeg", "-version").redirectErrorStream(true).start();
            try {
                boolean done = p.waitFor(1500, java.util.concurrent.TimeUnit.MILLISECONDS);
                ok = done ? (p.exitValue() == 0) : true;
            } finally {
                try { p.destroyForcibly(); } catch (Throwable ignore) {}
            }
        } catch (Throwable t) {
            ok = false;
        }
        cache = ok;
        lastCheck = now;
        return ok;
    }
}

