package bulu.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Iterator;

/**
 * Implementation of {@link BitKey} with more than 64 bits. Similar to
 * {@link java.util.BitSet}, but does not require dynamic resizing.
 */
public class BigBitKey extends AbstractBitKey {
	private static final long serialVersionUID = 62019791006L;
	
    protected long[] bits;

    public BigBitKey() {
    	
    }
    
    BigBitKey(int size) {
        bits = new long[chunkCount(size + 1)];
    }

    BigBitKey(BigBitKey big) {
        bits = big.bits.clone();
    }

    private int size() {
        return bits.length;
    }

    /**
     * Returns the number of chunks, ignoring any chunks on the leading
     * edge that are all zero.
     *
     * @return number of chunks that are not on the leading edge
     */
    int effectiveSize() {
        int n = bits.length;
        while (n > 0 && bits[n - 1] == 0) {
            --n;
        }
        return n;
    }

    public void set(int pos) {
        bits[chunkPos(pos)] |= bit(pos);
    }

    public boolean get(int pos) {
        return (bits[chunkPos(pos)] & bit(pos)) != 0;
    }

    public void clear(int pos) {
        bits[chunkPos(pos)] &= ~bit(pos);
    }

    public void clear() {
        for (int i = 0; i < bits.length; i++) {
            bits[i] = 0;
        }
    }

    public int cardinality() {
        int n = 0;
        for (int i = 0; i < bits.length; i++) {
            n += bitCount(bits[i]);
        }
        return n;
    }

    void or(long bits0) {
        this.bits[0] |= bits0;
    }

    void or(long bits0, long bits1) {
        this.bits[0] |= bits0;
        this.bits[1] |= bits1;
    }

    void or(long[] bits) {
        for (int i = 0; i < bits.length; i++) {
            this.bits[i] |= bits[i];
        }
    }

    void orNot(long bits0) {
        this.bits[0] ^= bits0;
    }

    void orNot(long bits0, long bits1) {
        this.bits[0] ^= bits0;
        this.bits[1] ^= bits1;
    }

    private void orNot(long[] bits) {
        for (int i = 0; i < bits.length; i++) {
            this.bits[i] ^= bits[i];
        }
    }

    private void and(long[] bits) {
        int length = Math.min(bits.length, this.bits.length);
        for (int i = 0; i < length; i++) {
            this.bits[i] &= bits[i];
        }
        for (int i = bits.length; i < this.bits.length; i++) {
            this.bits[i] = 0;
        }
    }

    public BitKey or(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.or(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.or(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            if (other.size() > size()) {
                final BigBitKey bk = (BigBitKey) other.copy();
                bk.or(bits);
                return bk;
            } else {
                final BigBitKey bk = (BigBitKey) copy();
                bk.or(other.bits);
                return bk;
            }
        }

        throw createException(bitKey);
    }

    public BitKey orNot(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.orNot(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.orNot(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            if (other.size() > size()) {
                final BigBitKey bk = (BigBitKey) other.copy();
                bk.orNot(bits);
                return bk;
            } else {
                final BigBitKey bk = (BigBitKey) copy();
                bk.orNot(other.bits);
                return bk;
            }
        }

        throw createException(bitKey);
    }

    public BitKey and(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey bk = (SmallBitKey) bitKey.copy();
            bk.and(bits[0]);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey bk = (MiddleBitKey) bitKey.copy();
            bk.and(bits[0], bits[1]);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            if (other.size() < size()) {
                final BigBitKey bk = (BigBitKey) other.copy();
                bk.and(bits);
                return bk;
            } else {
                final BigBitKey bk = (BigBitKey) copy();
                bk.and(other.bits);
                return bk;
            }
        }

        throw createException(bitKey);
    }

    public BitKey andNot(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.andNot(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.andNot(other.bits0, other.bits1);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) copy();
            bk.andNot(other.bits);
            return bk;
        }

        throw createException(bitKey);
    }

    private void andNot(long[] bits) {
        for (int i = 0; i < bits.length; i++) {
            this.bits[i] &= ~bits[i];
        }
    }

    private void andNot(long bits0, long bits1) {
        this.bits[0] &= ~bits0;
        this.bits[1] &= ~bits1;
    }

    private void andNot(long bits) {
        this.bits[0] &= ~bits;
    }

    public boolean isSuperSetOf(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) bitKey;
            return ((this.bits[0] | other.bits) == this.bits[0]);

        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) bitKey;
            return ((this.bits[0] | other.bits0) == this.bits[0])
                && ((this.bits[1] | other.bits1) == this.bits[1]);

        } else if (bitKey instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) bitKey;

            int len = Math.min(bits.length, other.bits.length);
            for (int i = 0; i < len; i++) {
                if ((this.bits[i] | other.bits[i]) != this.bits[i]) {
                    return false;
                }
            }
            if (other.bits.length > this.bits.length) {
                for (int i = len; i < other.bits.length; i++) {
                    if (other.bits[i] != 0) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    public boolean intersects(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) bitKey;
            return (this.bits[0] & other.bits) != 0;

        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) bitKey;
            return (this.bits[0] & other.bits0) != 0
                || (this.bits[1] & other.bits1) != 0;

        } else if (bitKey instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) bitKey;

            int len = Math.min(bits.length, other.bits.length);
            for (int i = 0; i < len; i++) {
                if ((this.bits[i] & other.bits[i]) != 0) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    public BitSet toBitSet() {
        final BitSet bitSet = new BitSet(64);
        int pos = 0;
        for (int i = 0; i < bits.length; i++) {
            copyFromLong(bitSet, pos, bits[i]);
            pos += 64;
        }
        return bitSet;
    }

    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            long[] bits = BigBitKey.this.bits.clone();
            int pos = -1;
            int index = 0;
            public boolean hasNext() {
                if (index >= bits.length) {
                    return false;
                }
                if (pos < 0) {
                    while (bits[index] == 0) {
                        index++;
                        if (index >= bits.length) {
                            return false;
                        }
                    }
                    pos = (64 * index) - 1;
                }
                long bs = bits[index];
                if (bs == 0) {
                    while (bits[index] == 0) {
                        index++;
                        if (index >= bits.length) {
                            return false;
                        }
                    }
                    pos = (64 * index) - 1;
                    bs = bits[index];
                }
                if (bs != 0) {
                    if (bs == Long.MIN_VALUE) {
                        pos = (64 * index) + 63;
                        bits[index] = 0;
                        return true;
                    }
                    long b = (bs&-bs);
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
                    bits[index] = bits[index] >>> (p + 1);
                    return true;
                }
                return false;
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

        int u = chunkPos(fromIndex);
        if (u >= bits.length) {
            return -1;
        }
        long word = bits[u] & (WORD_MASK << fromIndex);

        while (true) {
            if (word != 0) {
                return (u * 64) + Long.numberOfTrailingZeros(word);
            }
            if (++u == bits.length) {
                return -1;
            }
            word = bits[u];
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) o;
            if (this.bits[0] != other.bits) {
                return false;
            } else {
                for (int i = 1; i < this.bits.length; i++) {
                    if (this.bits[i] != 0) {
                        return false;
                    }
                }
                return true;
            }

        } else if (o instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) o;
            if (this.bits[0] != other.bits0) {
                return false;
            } else if (this.bits[1] != other.bits1) {
                return false;
            } else {
                for (int i = 2; i < this.bits.length; i++) {
                    if (this.bits[i] != 0) {
                        return false;
                    }
                }
                return true;
            }

        } else if (o instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) o;

            int len = Math.min(bits.length, other.bits.length);
            for (int i = 0; i < len; i++) {
                if (this.bits[i] != other.bits[i]) {
                    return false;
                }
            }
            if (this.bits.length > other.bits.length) {
                for (int i = len; i < this.bits.length; i++) {
                    if (this.bits[i] != 0) {
                        return false;
                    }
                }
            } else if (other.bits.length > this.bits.length) {
                for (int i = len; i < other.bits.length; i++) {
                    if (other.bits[i] != 0) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // It is important that leading 0s, and bits.length do not affect
        // the hash code. For instance, we want {1} to be equal to
        // {1, 0, 0}. This algorithm in fact ignores all 0s.
        //
        // It is also important that the hash code is the same as produced
        // by Small and Mid128.
        long h = 1234;
        for (int i = bits.length; --i >= 0;) {
            h ^= bits[i] * (i + 1);
        }
        return (int)((h >> 32) ^ h);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(64);
        buf.append("0x");
        int start = bits.length * 64 - 1;
        for (int i = start; i >= 0; i--) {
            buf.append((get(i)) ? '1' : '0');
        }
        return buf.toString();
    }

    public BitKey copy() {
        return new BigBitKey(this);
    }

    public BitKey emptyCopy() {
        return new BigBitKey(bits.length << ChunkBitCount);
    }

    public boolean isEmpty() {
        for (long bit : bits) {
            if (bit != 0) {
                return false;
            }
        }
        return true;
    }

    public int compareTo(BitKey bitKey) {
        if (bitKey instanceof BigBitKey) {
            return compareUnsignedArrays(this.bits, ((BigBitKey) bitKey).bits);
        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey that = (MiddleBitKey) bitKey;
            return -that.compareToBig(this);
        } else {
            SmallBitKey that = (SmallBitKey) bitKey;
            return -that.compareToBig(this);
        }
    }

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// long[] bits
		this.bits = (long[]) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// long[] bits
		out.writeObject(this.bits);
	}
}
