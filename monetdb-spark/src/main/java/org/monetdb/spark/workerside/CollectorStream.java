package org.monetdb.spark.workerside;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

public final class CollectorStream extends BufferedOutputStream {
    private static final int BUFFERSIZE = 65_536;
    private Runnable onResetCallback;

    public CollectorStream(OutputStream inner) {
        super(inner, BUFFERSIZE);
    }

    public void onReset(Runnable callback) {
        onResetCallback = callback;
    }

    OutputStream getInner() {
        return out;
    }

    OutputStream setInner(OutputStream newInner) {
        OutputStream prevInner = out;
        out = newInner;
        if (onResetCallback != null)
            onResetCallback.run();
        return prevInner;
    }
}
