package bulu.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Iterator;

/**
 * Implementation of {@link BitKey} for bit counts less than 64.
 */
public class SmallBitKey extends AbstractBitKey {
	private static final long serialVersionUID = 62019791008L;
	
    long bits;

    /**
     * Creates a Small with no bits set.
     */
    public SmallBitKey() {
    }

    /**
     * Creates a Small and initializes it to the 64 bit value.
     *
     * @param bits 64 bit value
     */
    SmallBitKey(long bits) {
        this.bits = bits;
    }

    public void set(int pos) {
        if (pos < 64) {
            bits |= bit(pos);
        } else {
            throw new IllegalArgumentException(
                "pos " + pos + " exceeds capacity 64");
        }
    }

    public boolean get(int pos) {
        return pos < 64 && ((bits & bit(pos)) != 0);
    }

    public void clear(int pos) {
        bits &= ~bit(pos);
    }

    public void clear() {
        bits = 0;
    }

    public int cardinality() {
        return bitCount(bits);
    }

    private void or(long bits) {
        this.bits |= bits;
    }

    private void orNot(long bits) {
        this.bits ^= bits;
    }

    void and(long bits) {
        this.bits &= bits;
    }

    public BitKey or(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.or(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) other.copy();
            bk.or(this.bits, 0);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) other.copy();
            bk.or(this.bits);
            return bk;
        }

        throw createException(bitKey);
    }

    public BitKey orNot(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.orNot(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final MiddleBitKey bk = (MiddleBitKey) other.copy();
            bk.orNot(this.bits, 0);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final BigBitKey bk = (BigBitKey) other.copy();
            bk.orNot(this.bits);
            return bk;
        }

        throw createException(bitKey);
    }

    public BitKey and(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.and(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.and(other.bits0);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.and(other.bits[0]);
            return bk;
        }

        throw createException(bitKey);
    }

    public BitKey andNot(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            final SmallBitKey other = (SmallBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.andNot(other.bits);
            return bk;

        } else if (bitKey instanceof MiddleBitKey) {
            final MiddleBitKey other = (MiddleBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.andNot(other.bits0);
            return bk;

        } else if (bitKey instanceof BigBitKey) {
            final BigBitKey other = (BigBitKey) bitKey;
            final SmallBitKey bk = (SmallBitKey) copy();
            bk.andNot(other.bits[0]);
            return bk;
        }

        throw createException(bitKey);
    }

    private void andNot(long bits) {
        this.bits &= ~bits;
    }

    public boolean isSuperSetOf(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) bitKey;
            return ((this.bits | other.bits) == this.bits);

        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) bitKey;
            return ((this.bits | other.bits0) == this.bits)
                && (other.bits1 == 0);

        } else if (bitKey instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) bitKey;
            if ((this.bits | other.bits[0]) != this.bits) {
                return false;
            } else {
                for (int i = 1; i < other.bits.length; i++) {
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
            return (this.bits & other.bits) != 0;

        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) bitKey;
            return (this.bits & other.bits0) != 0;

        } else if (bitKey instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) bitKey;
            return (this.bits & other.bits[0]) != 0;
        }
        return false;
    }

    public BitSet toBitSet() {
        final BitSet bitSet = new BitSet(64);
        long x = bits;
        int pos = 0;
        while (x != 0) {
            copyFromByte(bitSet, pos, (byte) (x & 0xff));
            x >>>= 8;
            pos += 8;
        }
        return bitSet;
    }

    
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            int pos = -1;
            long bits = SmallBitKey.this.bits;
            public boolean hasNext() {
                if (bits == 0) {
                    return false;
                }
                // This is a special case
                // Long.MIN_VALUE == -9223372036854775808
                if (bits == Long.MIN_VALUE) {
                    pos = 63;
                    bits = 0;
                    return true;
                }
                long b = (bits & -bits);
                if (b == 0) {
                    // should never happen
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
                    // first time
                    pos = p;
                } else if (p == 0) {
                    pos++;
                } else {
                    pos += (p + 1);
                }
                bits = bits >>> (p + 1);
                return true;
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

        if (fromIndex < 64) {
            long word = bits & (WORD_MASK << fromIndex);
            if (word != 0) {
                return Long.numberOfTrailingZeros(word);
            }
        }
        return -1;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof SmallBitKey) {
            SmallBitKey other = (SmallBitKey) o;
            return (this.bits == other.bits);

        } else if (o instanceof MiddleBitKey) {
            MiddleBitKey other = (MiddleBitKey) o;
            return (this.bits == other.bits0) && (other.bits1 == 0);

        } else if (o instanceof BigBitKey) {
            BigBitKey other = (BigBitKey) o;
            if (this.bits != other.bits[0]) {
                return false;
            } else {
                for (int i = 1; i < other.bits.length; i++) {
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
        return (int)(1234L ^ bits ^ (bits >>> 32));
    }

    public int compareTo(BitKey bitKey) {
        if (bitKey instanceof SmallBitKey) {
            SmallBitKey that = (SmallBitKey) bitKey;
            return this.bits == that.bits ? 0
                : this.bits < that.bits ? -1
                : 1;
        } else if (bitKey instanceof MiddleBitKey) {
            MiddleBitKey that = (MiddleBitKey) bitKey;
            if (that.bits1 != 0) {
                return -1;
            }
            return compareUnsigned(this.bits, that.bits0);
        } else {
            return compareToBig((BigBitKey) bitKey);
        }
    }

    protected int compareToBig(BigBitKey that) {
        int thatBitsLength = that.effectiveSize();
        switch (thatBitsLength) {
        case 0:
            return this.bits == 0 ? 0 : 1;
        case 1:
            return compareUnsigned(this.bits, that.bits[0]);
        default:
            return -1;
        }
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(64);
        buf.append("0x");
        for (int i = 63; i >= 0; i--) {
            buf.append((get(i)) ? '1' : '0');
        }
        return buf.toString();
    }

    public BitKey copy() {
        return new SmallBitKey(this.bits);
    }

    public BitKey emptyCopy() {
        return new SmallBitKey();
    }

    public boolean isEmpty() {
        return bits == 0;
    }

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// long bits
		this.bits = in.readLong();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// long bits
		out.writeLong(this.bits);
	}
}
