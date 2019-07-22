package com.zhongyou.protocol.parser;

//import AdvancedParser;
//import com.zhongyou.amplifier.protocol.Packet;

import org.junit.Test;

public class AdvancedParserTest {

    @Test
    public void receive() {
//        AdvancedParser advancedParser = new AdvancedParser(8000, Packet.PACKET_DESCRIPTOR);
//        List<byte[]> packets = new ArrayList<>();
//        advancedParser.setPacketReceiver((bytes, start, length) -> {
//            byte[] packet = new byte[length];
//            System.arraycopy(bytes, start, packet, 0, length);
//            packets.add(packet);
//        });
//        byte[] frame = {0x55, 0x00, 0x32, 0x1d, 0x31, 0x39, 0x32, 0x2e, 0x31, 0x36, 0x38, 0x2e, 0x33, 0x31, 0x2e, 0x31, 0x38, 0x34, 0x3a, 0x31, 0x36, 0x36, 0x31, 0x31, 0x3a, 0x32, 0x30, 0x31, 0x33, 0x30, 0x36, 0x35, 0x34, 0x72};
//
//        int count = 1000;
//        Random random = new Random();
//        for (int i = 0; i < count; i++) {
//            int start = 0;
//            for (int offset = 0; start < frame.length && start + offset <= frame.length; start += offset, offset = random.nextInt(frame.length - start + 1)) {
//                advancedParser.receive(frame, start, offset);
////                System.out.println(String.format("start %1d offset %2d", start, offset));
//            }
//        }
//        Assert.assertEquals(count, packets.size());
//        int looper = 1;
//        for (byte[] packet : packets) {
//            System.out.println("Packet " + (looper++));
//            for (int i = 0; i < frame.length; i++) {
//                Assert.assertEquals(frame[i], packet[i]);
//            }
//        }
    }
}