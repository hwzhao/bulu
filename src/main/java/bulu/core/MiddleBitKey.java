package bulu.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Iterator;

/**
 * Implementation of {@link BitKey} good for sizes less than 128.
 */
public class MiddleBitKey extends AbstractBitKey {
	private static final long serialVersionUID = 62019791007L;
	
    long bits0;
    long bits1;

    public MiddleBitKey() {
    }

    MiddleBitKey(MiddleBitKey mid) {
        this.bits0 = mid.bits0;
        this.bits1 = mid.bits1;
    }

    public void set(int pos) {
        if (pos < 64) {
            bits0 |= bit(pos);
        } else if (pos < 128) {
            bits1 |= bit(pos);
        } else {
            throw new IllegalArgumentException(
                "pos " + pos + " exceeds capacity 128");
        }
    }

    public boolean get(int pos) {
        if (pos < 64) {
            return (bits0 & bit(pos)) != 0;
        } else if (pos < 128) {
            return (bits1 & bit(pos)) != 0;
        } else {
            return false;
        }
    }

    public void clear(int pos) {
        if (pos < 64) {
            bits0 &= ~bit(pos);
        } else if (pos < 128) {
            bits1 &= ~bit(pos);
        } else {
            throw new IndexOutOfBoundsException(
                "pos " + pos + " exceeds size " + 128);
        }
    }

    public void clear() {
        bits0 = 0;
        bits1 = 0;
    }

    public int cardinality() {
        return bitCount(bits0)
           + bitCount(bits1);
    }

    void or(long bits0, long bits1) {
        this.bits0 |= bits0;
        this.bits1 |= bits1;
    }

    void orNot(long bits0, long bits1) {
        this.bits0 ^= bits0;
        this.bits1 ^= bits1;
    }

    void and(long bits0, long bits1) {
        this.bits0 &= bits0;
        this.bits1 &= bits1;
    }

    public BitKey or(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.or(other.bits, 0);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.or(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) other.copy();
            bk.or(this.bits0, this.bits1);
            return bk;
        }

        throw createException(bitKey);
    }

    public BitKey orNot(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.orNot(other.bits, 0);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.orNot(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) other.copy();
            bk.orNot(this.bits0, this.bits1);
            return bk;
        }

        throw createException(bitKey);
    }

    public BitKey and(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.and(other.bits, 0);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.and(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.and(other.bits[0], other.bits[1]);
            return bk;
        }

        throw createException(bitKey);
    }

    public BitKey andNot(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.andNot(other.bits, 0);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.andNot(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) copy();
            bk.andNot(other.bits[0], other.bits[1]);
            return bk;
        }

        throw createException(bitKey);
    }

    private void andNot(long bits0, long bits1) {
        this.bits0 &= ~bits0;
        this.bits1 &= ~bits1;
    }

    public boolean isSuperSetOf(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) bitKey;
            return ((this.bits0 | other.bits) == this.bits0);

        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) bitKey;
            return ((this.bits0 | other.bits0) == this.bits0)
                && ((this.bits1 | other.bits1) == this.bits1);

        } else if (bitKey instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) bitKey;
            if ((this.bits0 | other.bits[0]) != this.bits0) {
                return false;
            } else if ((this.bits1 | other.bits[1]) != this.bits1) {
                return false;
            } else {
                for (int i = 2; i < other.bits.length; i++) {
                    if (other.bits[i] != 0) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public boolean intersects(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) bitKey;
            return (this.bits0 & other.bits) != 0;

        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) bitKey;
            return (this.bits0 & other.bits0) != 0
                || (this.bits1 & other.bits1) != 0;

        } else if (bitKey instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) bitKey;
            if ((this.bits0 & other.bits[0]) != 0) {
                return true;
            } else if ((this.bits1 & other.bits[1]) != 0) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public BitSet toBitSet() {
        final BitSet bitSet = new BitSet(128);
        copyFromLong(bitSet, 0, bits0);
        copyFromLong(bitSet, 64, bits1);
        return bitSet;
    }
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            long bits0 = MiddleBitKey.this.bits0;
            long bits1 = MiddleBitKey.this.bits1;
            int pos = -1;
            public boolean hasNext() {
                if (bits0 != 0) {
                    if (bits0 == Long.MIN_VALUE) {
                        pos = 63;
                        bits0 = 0;
                        return true;
                    }
                    long b = (bits0&-bits0);
                    int delta = 0;
                    while (b >= 256) {
                        b = (b >> 8);
                        delta += 8;
                    }
                    int p = bitPositionTable[(int) b];
                    if (p >= 0) {
                        p += delta;
                    } else {
                        p = delta;
                    }
                    if (pos < 0) {
                        pos = p;
                    } else if (p == 0) {
                        pos++;
                    } else {
                        pos += (p + 1);
                    }
                    bits0 = bits0 >>> (p + 1);
                    return true;
                } else {
                    if (pos < 63) {
                        pos = 63;
                    }
                    if (bits1 == Long.MIN_VALUE) {
                        pos = 127;
                        bits1 = 0;
                        return true;
                    }
                    long b = (bits1&-bits1);
                    if (b == 0) {
                        return false;
                    }
                    int delta = 0;
                    while (b >= 256) {
                        b = (b >> 8);
                        delta += 8;
                    }
                    int p = bitPositionTable[(int) b];
                    if (p >= 0) {
                        p += delta;
                    } else {
                        p = delta;
                    }
                    if (pos < 0) {
                        pos = p;
                    } else if (p == 63) {
                        pos++;
                    } else {
                        pos += (p + 1);
                    }
                    bits1 = bits1 >>> (p + 1);
                    return true;
                }
            }
            public Integer next() {
                return Integer.valueOf(pos);
            }
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };
    }

    public int nextSetBit(int fromIndex) {
        if (fromIndex < 0) {
            throw new IndexOutOfBoundsException(
                "fromIndex < 0: " + fromIndex);
        }

        int u = fromIndex >> 6;
        long word;
        switch (u) {
        case 0:
            word = bits0 & (WORD_MASK << fromIndex);
            if (word != 0) {
                return Long.numberOfTrailingZeros(word);
            }
            word = bits1;
            if (word != 0) {
                return 64 + Long.numberOfTrailingZeros(word);
            }
            return -1;
        case 1:
            word = bits1 & (WORD_MASK << fromIndex);
            if (word != 0) {
                return 64 + Long.numberOfTrailingZeros(word);
            }
            return -1;
        default:
            return -1;
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) o;
            return (this.bits0 == other.bits) && (this.bits1 == 0);

        } else if (o instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) o;
            return (this.bits0 == other.bits0)
                && (this.bits1 == other.bits1);

        } else if (o instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) o;
            if (this.bits0 != other.bits[0]) {
                return false;
            } else if (this.bits1 != other.bits[1]) {
                return false;
            } else {
                for (int i = 2; i < other.bits.length; i++) {
                    if (other.bits[i] != 0) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        long h = 1234;
        h ^= bits0;
        h ^= bits1 * 2;
        return (int)((h >> 32) ^ h);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(64);
        buf.append("0x");
        for (int i = 127; i >= 0; i--) {
            buf.append((get(i)) ? '1' : '0');
        }
        return buf.toString();
    }

    public BitKey copy() {
        return new MiddleBitKey(this);
    }

    public BitKey emptyCopy() {
        return new MiddleBitKey();
    }

    public boolean isEmpty() {
        return bits0 == 0
            && bits1 == 0;
    }

    // implement Comparable (in lazy, expensive fashion)
    public int compareTo(BitKey bitKey) {
        if (bitKey instanceof MiddleBitKey) {
        	MiddleBitKey that = (MiddleBitKey) bitKey;
            if (this.bits1 != that.bits1) {
                return compareUnsigned(this.bits1, that.bits1);
            }
            return compareUnsigned(this.bits0, that.bits0);
        } else if (bitKey instanceof SmallBitKey) {
        	SmallBitKey that = (SmallBitKey) bitKey;
            if (this.bits1 != 0) {
                return 1;
            }
            return compareUnsigned(this.bits0, that.bits);
        } else {
            return compareToBig((BigBitKey) bitKey);
        }
    }

    int compareToBig(BigBitKey that) {
        int thatBitsLength = that.effectiveSize();
        switch (thatBitsLength) {
        case 0:
            return this.bits1 == 0
                && this.bits0 == 0
                ? 0
                : 1;
        case 1:
            if (this.bits1 != 0) {
                return 1;
            }
            return compareUnsigned(this.bits0, that.bits[0]);
        case 2:
            if (this.bits1 != that.bits[1]) {
                return compareUnsigned(this.bits1, that.bits[1]);
            }
            return compareUnsigned(this.bits0, that.bits[0]);
        default:
            return -1;
        }
    }

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// long bits0
		this.bits0 = in.readLong();
		
		// long bits1
		this.bits1 = in.readLong();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// long bits0
		out.writeLong(this.bits0);
		
		// long bits1
		out.writeLong(this.bits1);
	}
}