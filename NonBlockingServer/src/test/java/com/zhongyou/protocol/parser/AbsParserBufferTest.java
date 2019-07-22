package com.zhongyou.protocol.parser;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class AbsParserBufferTest {

    private Random random = new Random();

    @Test
    public void offer() {
        for (int test = 0; test < 10000; test++) {
            int bufferSize = 8000;
            AbsParserBuffer absParserBuffer = new AbsParserBuffer(bufferSize);
            byte[] buffer = new byte[bufferSize];
            int counter = 0;
            int rec = random.nextInt(500);
            for (int i = 1; (counter += rec) <= absParserBuffer.getBufferSize(); rec = random.nextInt(500), i++) {
                absParserBuffer.offer(buffer, 0, rec);
                Assert.assertEquals(counter, absParserBuffer.getCachedBytes());
                System.out.println(String.format("Round %1d receive %2d current %3d", i, rec, counter));
            }
        }
    }

//    @Test
//    public void receiveOverflow() {
//        int bufferSize = 8000;
//        AbsParserBuffer absParser = new AbsParserBuffer(bufferSize);
//        byte[] buffer = new byte[bufferSize];
//        int counter = 0;
//        int rec = bufferSize / 2;
//        for (int i = 1; (counter += rec) > 0; rec = bufferSize / 2, i++) {
//            absParser.receive(buffer, 0, rec);
//            Assert.assertEquals(counter, absParser.getCachedBytes());
//            System.out.println(String.format("Round %1d receive %2d current %3d", i, rec, counter));
//        }
//    }

    @Test
    public void skipBytes() {
        for (int test = 0; test < 100; test++) {
            int bufferSize = 8000;
            AbsParserBuffer absParserBuffer = new AbsParserBuffer(bufferSize);
            byte[] buffer = new byte[bufferSize];
            int rec = random.nextInt(500);
            int counter = 0;
            for (int i = 1; i < 10000; rec = random.nextInt(500), i++) {
                counter += rec;
                absParserBuffer.offer(buffer, 0, rec);
                int cachedBefore = absParserBuffer.getCachedBytes();
                Assert.assertEquals(counter, cachedBefore);
                int consumption = random.nextInt(Math.max(cachedBefore, 1));
                absParserBuffer.skip(consumption);
                int cacheAfter = absParserBuffer.getCachedBytes();
                counter -= consumption;
                Assert.assertEquals(counter, cacheAfter);
                System.out.println(String.format("Round %1d receive %2d cached before %3d consumption %4d cached after %5d", i, rec, cachedBefore, consumption, cacheAfter));
            }
        }
    }

    @Test
    public void skipBytesOverflow() {
        int bufferSize = 8000;
        AbsParserBuffer absParserBuffer = new AbsParserBuffer(bufferSize);
        byte[] buffer = new byte[bufferSize];
        int rec = random.nextInt(500);
        int counter = 0;
        for (int i = 1; i < 10000; rec = random.nextInt(500), i++) {
            counter += rec;
            absParserBuffer.offer(buffer, 0, rec);
            int cachedBefore = absParserBuffer.getCachedBytes();
            Assert.assertEquals(counter, cachedBefore);
            int consumption = cachedBefore;
            absParserBuffer.skip(consumption);
            int cacheAfter = absParserBuffer.getCachedBytes();
            counter -= consumption;
            Assert.assertEquals(counter, cacheAfter);
            System.out.println(String.format("Round %1d receive %2d cached before %3d consumption %4d cached after %5d", i, rec, cachedBefore, consumption, cacheAfter));
        }
    }

    @Test
    public void consumeBytes() {
        for (int test = 0; test < 100; test++) {
            int bufferSize = 8000;
            AbsParserBuffer absParserBuffer = new AbsParserBuffer(bufferSize);
            byte[] buffer = new byte[bufferSize];
            byte[] consumptionContainer = new byte[bufferSize];
            int rec = random.nextInt(500);
            int counter = 0;
            for (int i = 1; i < 10000; rec = random.nextInt(500), i++) {
                counter += rec;
                for (int j = 0; j < rec; j++) {
                    buffer[j] = (byte) random.nextInt(256);
                }
                absParserBuffer.offer(buffer, 0, rec);
                int cachedBefore = absParserBuffer.getCachedBytes();
                Assert.assertEquals(counter, cachedBefore);
                int consumption = rec;
                absParserBuffer.consume(consumption, consumptionContainer, 0);
                int cacheAfter = absParserBuffer.getCachedBytes();
                counter -= consumption;
                Assert.assertEquals(counter, cacheAfter);
                for (int j = 0; j < consumption; j++) {
                    Assert.assertEquals(buffer[j], consumptionContainer[j]);
                }
                System.out.println(String.format("Round %1d receive %2d cached before %3d consumption %4d cached after %5d", i, rec, cachedBefore, consumption, cacheAfter));
            }
        }
    }

    @Test
    public void peekBytes() {
        for (int test = 0; test < 100; test++) {
            int bufferSize = 8000;
            AbsParserBuffer absParserBuffer = new AbsParserBuffer(bufferSize);
            byte[] buffer = new byte[bufferSize];
            byte[] peekContainer = new byte[bufferSize];
            int rec = random.nextInt(500);
            int counter = 0;
            for (int i = 1; i < 10000; rec = random.nextInt(500), i++) {
                counter += rec;
                for (int j = 0; j < rec; j++) {
                    buffer[j] = (byte) random.nextInt(256);
                }
                absParserBuffer.offer(buffer, 0, rec);
                int cachedBefore = absParserBuffer.getCachedBytes();
                if (cachedBefore < 1) {
                    continue;
                }
                Assert.assertEquals(counter, cachedBefore);

                int peekStart = random.nextInt(cachedBefore);
                int peek = random.nextInt(cachedBefore - peekStart);
                absParserBuffer.peek(peekStart, peek, peekContainer, peekStart);
                absParserBuffer.skip(rec);
                int cacheAfter = absParserBuffer.getCachedBytes();
                counter -= rec;
                Assert.assertEquals(counter, cacheAfter);
                for (int j = peekStart; j < peekStart + peek; j++) {
                    Assert.assertEquals(buffer[j], peekContainer[j]);
                }
                System.out.println(String.format("Round %1d receive %2d cached before %3d consumption %4d cached after %5d", i, rec, cachedBefore, rec, cacheAfter));
            }
        }
    }
}