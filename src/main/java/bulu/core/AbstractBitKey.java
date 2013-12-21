package bulu.core;

import java.util.BitSet;


	/**
     * Abstract implementation of {@link BitKey}.
     */
    abstract class AbstractBitKey implements BitKey {
    	private static final long serialVersionUID = 62019791017L;
        // chunk is a long, which has 64 bits
        protected static final int ChunkBitCount = 6;
        protected static final int Mask = 63;
        protected static final long WORD_MASK = 0xffffffffffffffffL;

        /**
         * Creates a chunk containing a single bit.
         */
        protected static long bit(int pos) {
            return (1L << (pos & Mask));
        }

        /**
         * Returns which chunk a given bit falls into.
         * Bits 0 to 63 fall in chunk 0, bits 64 to 127 fall into chunk 1.
         */
        protected static int chunkPos(int size) {
            return (size >> ChunkBitCount);
        }

        /**
         * Returns the number of chunks required for a given number of bits.
         *
         * <p>0 bits requires 0 chunks; 1 - 64 bits requires 1 chunk; etc.
         */
        protected static int chunkCount(int size) {
            return (size + 63) >> ChunkBitCount;
        }

        /**
         * Returns the number of one-bits in the two's complement binary
         * representation of the specified <tt>long</tt> value.  This function
         * is sometimes referred to as the <i>population count</i>.
         *
         * @return the number of one-bits in the two's complement binary
         *     representation of the specified <tt>long</tt> value.
         * @since 1.5
         */
         protected static int bitCount(long i) {
            i = i - ((i >>> 1) & 0x5555555555555555L);
            i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
            i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
            i = i + (i >>> 8);
            i = i + (i >>> 16);
            i = i + (i >>> 32);
            return (int)i & 0x7f;
        }

        public final void set(int pos, boolean value) {
            if (value) {
                set(pos);
            } else {
                clear(pos);
            }
        }

        /**
         * Copies a byte into a bit set at a particular position.
         *
         * @param bitSet Bit set
         * @param pos Position
         * @param x Byte
         */
        protected static void copyFromByte(BitSet bitSet, int pos, byte x)
        {
            if (x == 0) {
                return;
            }
            if ((x & 0x01) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x02) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x04) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x08) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x10) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x20) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x40) != 0) {
                bitSet.set(pos, true);
            }
            ++pos;
            if ((x & 0x80) != 0) {
                bitSet.set(pos, true);
            }
        }

        /**
         * Copies a {@code long} value (interpreted as 64 bits) into a bit set.
         *
         * @param bitSet Bit set
         * @param pos Position
         * @param x Byte
         */
        protected static void copyFromLong(
            final BitSet bitSet,
            int pos,
            long x)
        {
            while (x != 0) {
                copyFromByte(bitSet, pos, (byte) (x & 0xff));
                x >>>= 8;
                pos += 8;
            }
        }

        protected IllegalArgumentException createException(BitKey bitKey) {
            final String msg = (bitKey == null)
                ? "Null BitKey"
                : "Bad BitKey type: " + bitKey.getClass().getName();
            return new IllegalArgumentException(msg);
        }

        /**
         * Compares a pair of {@code long} arrays, using unsigned comparison
         * semantics and padding to the left with 0s.
         *
         * <p>Values are treated as unsigned for the purposes of comparison.
         *
         * <p>If the arrays have different lengths, the shorter is padded with
         * 0s.
         *
         * @param a1 First array
         * @param a2 Second array
         * @return -1 if a1 compares less to a2,
         * 0 if a1 is equal to a2,
         * 1 if a1 is greater than a2
         */
        static int compareUnsignedArrays(long[] a1, long[] a2) {
            int i1 = a1.length - 1;
            int i2 = a2.length - 1;
            if (i1 > i2) {
                do {
                    if (a1[i1] != 0) {
                        return 1;
                    }
                    --i1;
                } while (i1 > i2);
            } else if (i2 > i1) {
                do {
                    if (a2[i2] != 0) {
                        return -1;
                    }
                    --i2;
                } while (i2 > i1);
            }
            assert i1 == i2;
            for (; i1 >= 0; --i1) {
                int c = compareUnsigned(a1[i1], a2[i1]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        /**
         * Performs unsigned comparison on two {@code long} values.
         *
         * @param i1 First value
         * @param i2 Second value
         * @return -1 if i1 is less than i2,
         * 1 if i1 is greater than i2,
         * 0 if i1 equals i2
         */
        static int compareUnsigned(long i1, long i2) {
            // We want to do unsigned comparison.
            // Signed comparison returns the correct result except
            // if i1<0 & i2>=0
            // or i1>=0 & i2<0
            if (i1 == i2) {
                return 0;
            } else if ((i1 < 0) == (i2 < 0)) {
                // Same signs, signed comparison gives the right result
                return i1 < i2 ? -1 : 1;
            } else {
                // Different signs, use signed comparison and invert the result
                return i1 < i2 ? 1 : -1;
            }
        }
        final static byte bitPositionTable[] = {
            -1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
             4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
         };
    }

    
