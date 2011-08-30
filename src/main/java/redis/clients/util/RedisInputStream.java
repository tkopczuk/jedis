/*
 * Copyright 2009-2010 MBTE Sweden AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis.clients.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisInputStream {
	protected SocketChannel channel;
	protected Selector selector;
	protected SelectionKey acceptKey;
	
    protected final ByteBuffer buffer;

    protected int count, limit;
    
    public RedisInputStream(SocketChannel channel, int size) throws IOException {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buffer = ByteBuffer.allocateDirect(size);
        buffer.limit(0);
        this.channel = channel;
        selector = Selector.open();
        acceptKey = channel.register( selector,SelectionKey.OP_READ);
    }

    public RedisInputStream(SocketChannel channel) throws IOException {
        this(channel, 8192);
    }
    
    public void close() throws IOException {
    	selector.close();
    }

    public byte readByte() throws IOException {
        if (buffer.remaining() == 0) {
            fill();
        }

        return buffer.get();
    }

    public String readLine() {
        int b;
        byte c;
        StringBuilder sb = new StringBuilder();

        try {
            while (true) {
                if (buffer.remaining() == 0) {
                    fill();
                }
                if (limit == -1)
                    break;

                b = buffer.get();
                if (b == '\r') {
                    if (buffer.remaining() == 0) {
                        fill();
                    }

                    if (limit == -1) {
                        sb.append((char) b);
                        break;
                    }

                    c = buffer.get();
                    if (c == '\n') {
                        break;
                    }
                    sb.append((char) b);
                    sb.append((char) c);
                } else {
                    sb.append((char) b);
                }
            }
        } catch (IOException e) {
            throw new JedisException(e);
        }
        String reply = sb.toString();
        if (reply.length() == 0) {
            throw new JedisConnectionException(
                    "It seems like server has closed the connection.");
        }
        return reply;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (buffer.remaining() == 0) {
            fill();
            if (limit == -1)
                return -1;
        }
        final int length = Math.min(buffer.remaining(), len);
        buffer.get(b, 0, length);
        return length;
    }

    private void fill() throws IOException {
    	buffer.clear();
    	selector.select();
    	Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
    	while (selectedKeys.hasNext()) {
            SelectionKey key = (SelectionKey) selectedKeys.next();
            selectedKeys.remove();

            if (!key.isValid()) {
            	continue;
            }

            if (key.isReadable()) {
    	        limit = channel.read(buffer);
    	        //System.out.println(lastLimit);
            }
        }
    	//System.out.print(limit);
    	//System.out.println(" bytes went through");
    	buffer.flip();
    }
}
