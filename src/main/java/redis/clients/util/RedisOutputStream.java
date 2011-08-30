package redis.clients.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * The class implements a buffered output stream without synchronization
 * There are also special operations like in-place string encoding.
 * This stream fully ignore mark/reset and should not be used outside Jedis
 */
public final class RedisOutputStream {
	protected SocketChannel channel;
	protected Selector selector;
	protected SelectionKey acceptKey;
    protected final ByteBuffer buffer;

    public RedisOutputStream(final SocketChannel channel) throws IOException {
        this(channel, 8192);
    }

    public RedisOutputStream(final SocketChannel channel, final int size) throws IOException {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buffer = ByteBuffer.allocateDirect(size);
        this.channel = channel;
        selector = Selector.open();
        acceptKey = channel.register( selector,SelectionKey.OP_WRITE);
    }
    
    public void close() throws IOException {
    	selector.close();
    }

    private void flushBuffer() throws IOException {
    	buffer.flip();
    	selector.select();
    	Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
    	while (selectedKeys.hasNext()) {
            SelectionKey key = (SelectionKey) selectedKeys.next();
            selectedKeys.remove();

            if (!key.isValid()) {
            	continue;
            }

            if (key.isWritable()) {
            	channel.write(buffer);
            }
          }
    	buffer.clear();
    }

    public void write(final byte b) throws IOException {
        if (!buffer.hasRemaining()) {
            flushBuffer();
        }
    	buffer.put(b);
    }
    
    public void write(final byte[] b) throws IOException {
    	write(b, 0, b.length);
    }

    public void write(final byte b[], final int off, final int len) throws IOException {
        if (len >= buffer.capacity()) {
        	System.out.println("FUCKED");
        } else {
            if (len >= buffer.remaining()) {
                flushBuffer();
            }
            buffer.put(b,off,len);
        }
    }

    public void writeAsciiCrLf(final String in) throws IOException {
        final int size = in.length();

        for (int i = 0; i != size; ++i) {
            buffer.put((byte) in.charAt(i));
            if (!buffer.hasRemaining()) {
                flushBuffer();
            }
        }

        writeCrLf();
    }

    public static boolean isSurrogate(final char ch) {
        return ch >= Character.MIN_SURROGATE && ch <= Character.MAX_SURROGATE;
    }

    public static int utf8Length (final String str) {
        int strLen = str.length(), utfLen = 0;
        for(int i = 0; i != strLen; ++i) {
            char c = str.charAt(i);
            if (c < 0x80) {
                utfLen++;
            } else if (c < 0x800) {
                utfLen += 2;
            } else if (isSurrogate(c)) {
                i++;
                utfLen += 4;
            } else {
                utfLen += 3;
            }
        }
        return utfLen;
    }

    public void writeCrLf() throws IOException {
        if (buffer.remaining() < 2) {
            flushBuffer();
        }

        buffer.put((byte)'\r');
        buffer.put((byte)'\n');
    }

    public void writeUtf8CrLf(final String str) throws IOException {
        int strLen = str.length();

        int i;
        for (i = 0; i < strLen; i++) {
            char c = str.charAt(i);
            if (!(c < 0x80)) break;
            buffer.put((byte) c);
            if(!buffer.hasRemaining()) {
                flushBuffer();
            }
        }

        for (; i < strLen; i++) {
            char c = str.charAt(i);
            if (c < 0x80) {
                buffer.put((byte) c);
                if(!buffer.hasRemaining()) {
                    flushBuffer();
                }
            } else if (c < 0x800) {
                if (buffer.remaining() < 2) {
                    flushBuffer();
                }
                buffer.put((byte)(0xc0 | (c >> 6)));
                buffer.put((byte)(0x80 | (c & 0x3f)));
            } else if (isSurrogate(c)) {
                if (buffer.remaining() < 4) {
                    flushBuffer();
                }
                int uc = Character.toCodePoint(c, str.charAt(i++));
                buffer.put(((byte)(0xf0 | ((uc >> 18)))));
                buffer.put(((byte)(0x80 | ((uc >> 12) & 0x3f))));
                buffer.put(((byte)(0x80 | ((uc >> 6) & 0x3f))));
                buffer.put(((byte)(0x80 | (uc & 0x3f))));
            } else {
                if (buffer.remaining() < 3) {
                    flushBuffer();
                }
                buffer.put(((byte)(0xe0 | ((c >> 12)))));
                buffer.put(((byte)(0x80 | ((c >> 6) & 0x3f))));
                buffer.put(((byte)(0x80 | (c & 0x3f))));
            }
        }

        writeCrLf();
    }

    private final static int[] sizeTable = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};

    private final static byte[] DigitTens = {
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
            '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
            '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
            '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
            '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
            '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
            '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
            '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
            '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
            '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
    };

    private final static byte[] DigitOnes = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    };

    private final static byte[] digits = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z'
    };

    public void writeIntCrLf(int value) throws IOException {
        if (value < 0) {
            write((byte)'-');
            value = -value;
        }

        int size = 0;
        while (value > sizeTable[size])
            size++;

        size++;
        if (size >= buffer.remaining()) {
            flushBuffer();
        }

        int q, r;
        int charPos = buffer.position() + size;

        while (value >= 65536) {
            q = value / 100;
            r = value - ((q << 6) + (q << 5) + (q << 2));
            value = q;
            buffer.put(--charPos, DigitOnes[r]);
            buffer.put(--charPos, DigitTens[r]);
        }

        for (; ;) {
            q = (value * 52429) >>> (16 + 3);
            r = value - ((q << 3) + (q << 1));
            buffer.put(--charPos, digits[r]);
            value = q;
            if (value == 0) break;
        }
        buffer.position(buffer.position()+size);

        writeCrLf();
    }

    public void flush() throws IOException {
        flushBuffer();
    }
}
