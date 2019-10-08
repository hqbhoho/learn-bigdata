package com.hqbhoho.bigdata.guava.io;

import com.google.common.io.Closer;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * describe:
 * <p>
 * Guava IO close Operation
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/25
 */
public class IOCloser {
    public static void main(String[] args) throws IOException {
        Closer closer = Closer.create();
        try {
            Reader r1 = closer.register(Files.asCharSource(new File("E:\\test.log"), Charset.defaultCharset()).openStream());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
}
