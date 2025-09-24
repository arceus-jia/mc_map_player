package me.example.mapframeplayer;

import java.io.IOException;
import java.io.InputStream;

class PpmReader {
    // Minimal PPM (P6) parser: reads one frame (binary P6 with maxval 255)
    static boolean readOnePPMFrame(InputStream in, int width, int height, byte[] out) throws IOException {
        if (!seekMagicP6(in)) return false;
        String wTok = readTokenSkippingComments(in);
        String hTok = readTokenSkippingComments(in);
        String maxTok = readTokenSkippingComments(in);
        if (wTok == null || hTok == null || maxTok == null) return false;
        int w, h, maxv;
        try {
            w = Integer.parseInt(wTok);
            h = Integer.parseInt(hTok);
            maxv = Integer.parseInt(maxTok);
        } catch (NumberFormatException e) {
            return false;
        }
        if (w != width || h != height || maxv != 255) {
            long toSkip = (long) w * (long) h * 3L;
            while (toSkip > 0) {
                long s = in.skip(toSkip);
                if (s <= 0) break;
                toSkip -= s;
            }
            return false;
        }
        int need = width * height * 3;
        int off = 0;
        while (off < need) {
            int n = in.read(out, off, need - off);
            if (n <= 0) break;
            off += n;
        }
        return off == need;
    }

    private static boolean seekMagicP6(InputStream in) throws IOException {
        int prev = -1;
        int c;
        while ((c = in.read()) != -1) {
            if (prev == 'P' && c == '6') {
                return true;
            }
            prev = c;
        }
        return false;
    }

    private static String readTokenSkippingComments(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while (true) {
            in.mark(1);
            c = in.read();
            if (c == -1) return null;
            if (Character.isWhitespace(c)) continue;
            if (c == '#') {
                while ((c = in.read()) != -1 && c != '\n') {}
                continue;
            }
            in.reset();
            break;
        }
        while (true) {
            c = in.read();
            if (c == -1) break;
            if (Character.isWhitespace(c)) break;
            sb.append((char) c);
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}

