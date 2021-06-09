package de.hhu.bsinfo.hadronio.util;

import java.util.PrimitiveIterator;
import java.util.Random;

public class TagUtil {

    public enum MessageType {
        DEFAULT((byte) 0),
        FLUSH((byte) 1);

        private final byte value;

        MessageType(byte value) {
            this.value = value;
        }

        byte getValue() {
            return value;
        }

        static MessageType fromByte(byte value) {
            switch (value) {
                case 0:
                    return DEFAULT;
                case 1:
                    return FLUSH;
                default:
                    throw new IllegalArgumentException("Invalid message type value " + value + "!");
            }
        }
    }

    public static final long TAG_MASK_FULL = 0xffffffffffffffffL;
    public static final long TAG_MASK_TARGET_ID = 0x00ffffffffffffffL;
    public static final long TAG_MASK_MESSAGE_TYPE = 0xff00000000000000L;

    private static final PrimitiveIterator.OfLong tagIterator = new Random().longs(0, TAG_MASK_TARGET_ID).distinct().iterator();

    private TagUtil() {}

    public static long generateId() {
        return tagIterator.nextLong();
    }

    public static long getTargetId(long tag) {
        return tag & TAG_MASK_TARGET_ID;
    }

    public static MessageType getMessageType(long tag) {
        return MessageType.fromByte((byte) ((tag & TAG_MASK_MESSAGE_TYPE) >> 56));
    }

    public static long setTargetId(long tag, long targetId) {
        return (tag & ~TAG_MASK_TARGET_ID) | targetId;
    }

    public static long setMessageType(long tag, MessageType messageType) {
        return (tag & ~TAG_MASK_MESSAGE_TYPE) | ((long) messageType.value << 56);
    }
}
