package com.github.luben.zstd;

import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.IndexOutOfBoundsException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import com.github.luben.zstd.util.Native;

/**
 * InputStream filter that decompresses the data provided
 * by the underlying InputStream using Zstd compression.
 *
 * It does not support mark/reset methods. It also does not have finalizer,
 * so if you rely on finalizers to clean the native memory and release
 * buffers use `ZstdInputStream` instead.
 */

public class ZstdInputStreamNoFinalizer extends FilterInputStream {

    static {
        Native.load();
    }

    // Opaque pointer to Zstd context object
    private final long stream;
    private long dstPos = 0;
    private long srcPos = 0;
    private long srcSize = 0;
    private boolean needRead = true;
    private final BufferPool bufferPool;
    private final ByteBuffer srcByteBuffer;
    private final byte[] src;

    private boolean isContinuous = false;
    private boolean frameFinished = true;
    private boolean isClosed = false;
    private boolean isMultiDict = false;
    // Dicts loaded while multi-dict mode was on. Native zstd keeps them in the DCtx's ddictSet,
    // which is only freed together with the DCtx (not by switching back to single-dict mode or by
    // loading another dict), so they must stay locked until close().
    private LinkedList<ZstdDictDecompress> active_dicts = new LinkedList<ZstdDictDecompress>();
    // The dict loaded while in single-dict mode, if any. Native only references it as the current
    // DCtx dict (never in ddictSet), so it can be released as soon as another dict replaces it.
    private ZstdDictDecompress singleDict = null;

    /* JNI methods */
    public static native long recommendedDInSize();
    public static native long recommendedDOutSize();
    private static native long createDStream();
    private static native int  freeDStream(long stream);
    private native int  initDStream(long stream);
    private native int  decompressStream(long stream, byte[] dst, int dst_size, byte[] src, int src_size);

    /**
     * create a new decompressing InputStream
     * @param inStream the stream to wrap
     */
    public ZstdInputStreamNoFinalizer(InputStream inStream) throws IOException {
        this(inStream, NoPool.INSTANCE);
    }

    /**
     * create a new decompressing InputStream
     * @param inStream the stream to wrap
     * @param bufferPool the pool to fetch and return buffers
     */
    public ZstdInputStreamNoFinalizer(InputStream inStream, BufferPool bufferPool) throws IOException {
        super(inStream);
        this.bufferPool = bufferPool;
        this.srcByteBuffer = Zstd.getArrayBackedBuffer(bufferPool, (int) recommendedDInSize());
        this.src = srcByteBuffer.array();
        // memory barrier
        synchronized(this) {
            this.stream = createDStream();
            initDStream(stream);
        }
    }

    /**
     * Don't break on unfinished frames
     *
     * Use case: decompressing files that are not yet finished writing and compressing
     */
    public synchronized ZstdInputStreamNoFinalizer setContinuous(boolean b) {
        isContinuous = b;
        return this;
    }

    public synchronized boolean getContinuous() {
        return this.isContinuous;
    }

    public synchronized ZstdInputStreamNoFinalizer setDict(byte[] dict) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        int size = Zstd.loadDictDecompress(stream, dict, dict.length);
        if (Zstd.isError(size)) {
            throw new ZstdIOException(size);
        }
        return this;
    }

    public synchronized ZstdInputStreamNoFinalizer setDict(ZstdDictDecompress dict) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        dict.acquireSharedLock();
        int size = Zstd.loadFastDictDecompress(stream, dict);
        if (Zstd.isError(size)) {
            long code = Zstd.getErrorCode(size);
            if (code == Zstd.errStageWrong() || code == Zstd.errDictionaryWrong()) {
                // failed before the DCtx was touched: native does not reference the dict
                dict.releaseSharedLock();
            } else {
                // ZSTD_DCtx_refDDict fails on growing ddictSet only after it has made this dict
                // the current one, so native may still use it: keep it locked until close()
                active_dicts.addFirst(dict);
            }
            throw new ZstdIOException(size);
        }
        // ZSTD_DCtx_refDDict dropped the previous current dict. If that one was loaded in
        // single-dict mode it is not in ddictSet, so native no longer references it.
        ZstdDictDecompress previous = singleDict;
        if (isMultiDict) {
            // native added it to ddictSet: keep it locked until close()
            active_dicts.addFirst(dict);
            singleDict = null;
        } else {
            singleDict = dict;
        }
        if (previous != null) {
            previous.releaseSharedLock();
        }
        return this;
    }

    public synchronized ZstdInputStreamNoFinalizer setLongMax(int windowLogMax) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        int size = Zstd.setDecompressionLongMax(stream, windowLogMax);
        if (Zstd.isError(size)) {
            throw new ZstdIOException(size);
        }
        return this;
    }

    public synchronized ZstdInputStreamNoFinalizer setRefMultipleDDicts(boolean useMultiple) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        int size = Zstd.setRefMultipleDDicts(stream, useMultiple);
        if (Zstd.isError(size)) {
            throw new ZstdIOException(size);
        }
        isMultiDict = useMultiple;
        return this;
    }

    public synchronized int read(byte[] dst, int offset, int len) throws IOException {
        // guard agains buffer overflows
        if (offset < 0 || len < 0 || len > dst.length - offset) {
            throw new IndexOutOfBoundsException("Requested length " + len
                    + " from offset " + offset + " in buffer of size " + dst.length);
        }
        if (len == 0) {
            return 0;
        } else {
            int result = 0;
            while (result == 0) {
                result = readInternal(dst, offset, len);
            }
            return result;
        }
    }

    int readInternal(byte[] dst, int offset, int len) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }

        // guard against buffer overflows
        if (offset < 0 || len < 0 || len > dst.length - offset) {
            throw new IndexOutOfBoundsException("Requested length " + len
                    + " from offset " + offset + " in buffer of size " + dst.length);
        }
        int dstSize = offset + len;
        dstPos = offset;
        long lastDstPos = -1;

        while (dstPos < dstSize && lastDstPos < dstPos) {
            // we will read only if data from the upstream is available OR
            // we have not yet produced any output
            if (needRead && (in.available() > 0 || dstPos == offset)) {
                srcSize = in.read(src, 0, src.length);
                srcPos = 0;
                if (srcSize < 0) {
                    srcSize = 0;
                    if (frameFinished) {
                        return -1;
                    } else if (isContinuous) {
                        srcSize = (int)(dstPos - offset);
                        if (srcSize > 0) {
                            return (int) srcSize;
                        }
                        return -1;
                    } else {
                        throw new ZstdIOException(Zstd.errCorruptionDetected(), "Truncated source");
                    }
                }
                frameFinished = false;
            }

            lastDstPos = dstPos;
            int size = decompressStream(stream, dst, dstSize, src, (int) srcSize);

            if (Zstd.isError(size)) {
                throw new ZstdIOException(size);
            }

            // we have completed a frame
            if (size == 0) {
                frameFinished = true;
                // we need to read from the upstream only if we have not consumed
                // fully the source buffer
                needRead = srcPos == srcSize;
                return (int)(dstPos - offset);
            } else {
                // size > 0, so more input is required but there is data left in
                // the decompressor buffers if we have not filled the dst buffer
                needRead = dstPos < dstSize;
            }
        }
        return (int)(dstPos - offset);
    }

    public synchronized int read() throws IOException {
        byte[] oneByte = new byte[1];
        int result = 0;
        while (result == 0) {
            result = readInternal(oneByte, 0, 1);
        }
        if (result == 1) {
            return oneByte[0] & 0xff;
        } else {
            return -1;
        }
    }

    public synchronized int available() throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        if (!needRead) {
            return 1;
        } else {
            return in.available();
        }
    }

    /* we don't support mark/reset */
    public boolean markSupported() {
        return false;
    }

    /* we can skip forward */
    public synchronized long skip(long numBytes) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        if (numBytes <= 0) {
            return 0;
        }
        int bufferLen = (int) recommendedDOutSize();
        if (bufferLen > numBytes) {
            bufferLen = (int) numBytes;
        }
        ByteBuffer buf = Zstd.getArrayBackedBuffer(bufferPool, bufferLen);
        long toSkip = numBytes;
        try {
            byte data[] = buf.array();
            while (toSkip > 0) {
                int read = read(data, 0, (int) Math.min((long) bufferLen, toSkip));
                if (read < 0) {
                    break;
                }
                toSkip -= read;
            }
        } finally {
            bufferPool.release(buf);
        }
        return numBytes - toSkip;
    }

    public synchronized void close() throws IOException {
        if (isClosed) {
            return;
        }
        while (active_dicts.size() > 0) {
            active_dicts.removeLast().releaseSharedLock();
        }
        if (singleDict != null) {
            singleDict.releaseSharedLock();
            singleDict = null;
        }
        isClosed = true;
        bufferPool.release(srcByteBuffer);
        freeDStream(stream);
        in.close();
    }
}
