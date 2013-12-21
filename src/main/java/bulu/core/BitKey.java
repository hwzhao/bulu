package bulu.core;

import java.io.Externalizable;
import java.util.BitSet;
import java.util.Iterator;

public interface BitKey extends Externalizable, Comparable<BitKey>, Iterable<Integer>{
	/**
     * The BitKey with no bits set.
     */
    BitKey EMPTY = Factory.makeBitKey(0);

    /**
     * Sets the bit at the specified index to the specified value.
     */
    void set(int bitIndex, boolean value);

    /**
     * Sets the bit at the specified index to <code>true</code>.
     */
    void set(int bitIndex);

    /**
     * Returns the value of the bit with the specified index. The value
     * is <code>true</code> if the bit with the index <code>bitIndex</code>
     * is currently set in this <code>BitKey</code>; otherwise, the result
     * is <code>false</code>.
     */
    boolean get(int bitIndex);

    /**
     * Sets the bit specified by the index to <code>false</code>.
     */
    void clear(int bitIndex);

    /**
     * Sets all of the bits in this BitKey to <code>false</code>.
     */
    void clear();

    /**
     * Is every bit set in the parameter <code>bitKey</code> also set in
     * <code>this</code>.
     * If one switches <code>this</code> with the parameter <code>bitKey</code>
     * one gets the equivalent of isSubSetOf.
     *
     * @param bitKey Bit key
     */
    boolean isSuperSetOf(BitKey bitKey);

    /**
     * Or the parameter <code>BitKey</code> with <code>this</code>.
     *
     * @param bitKey Bit key
     */
    BitKey or(BitKey bitKey);

    /**
     * XOr the parameter <code>BitKey</code> with <code>this</code>.
     *
     * @param bitKey Bit key
     */
    BitKey orNot(BitKey bitKey);

    /**
     * Returns the boolean AND of this bitkey and the given bitkey.
     *
     * @param bitKey Bit key
     */
    BitKey and(BitKey bitKey);

    /**
     * Returns a <code>BitKey</code> containing all of the bits in this
     * <code>BitSet</code> whose corresponding
     * bit is NOT set in the specified <code>BitSet</code>.
     */
    BitKey andNot(BitKey bitKey);

    /**
     * Returns a copy of this BitKey.
     *
     * @return copy of BitKey
     */
    BitKey copy();

    /**
     * Returns an empty BitKey of the same type. This is the same
     * as calling {@link #copy} followed by {@link #clear()}.
     *
     * @return BitKey of same type
     */
    BitKey emptyCopy();

    /**
     * Returns true if this <code>BitKey</code> contains no bits that are set
     * to <code>true</code>.
     */
    boolean isEmpty();

    /**
     * Returns whether this BitKey has any bits in common with a given BitKey.
     */
    boolean intersects(BitKey bitKey);

    /**
     * Returns a {@link BitSet} with the same contents as this BitKey.
     */
    BitSet toBitSet();

    /**
     * An Iterator over the bit positions.
     * For example, if the BitKey had positions 3 and 4 set, then
     * the Iterator would return the values 3 and then 4. The bit
     * positions returned by the iterator are in the order, from
     * smallest to largest, as they are set in the BitKey.
     */
    Iterator<Integer> iterator();

    /**
     * Returns the index of the first bit that is set to <code>true</code>
     * that occurs on or after the specified starting index. If no such
     * bit exists then -1 is returned.
     *
     * To iterate over the <code>true</code> bits in a <code>BitKey</code>,
     * use the following loop:
     *
     * <pre>
     * for (int i = bk.nextSetBit(0); i >= 0; i = bk.nextSetBit(i + 1)) {
     *     // operate on index i here
     * }</pre>
     *
     * @param   fromIndex the index to start checking from (inclusive)
     * @return  the index of the next set bit
     * @throws  IndexOutOfBoundsException if the specified index is negative
     */
    int nextSetBit(int fromIndex);

    /**
     * Returns the number of bits set.
     *
     * @return Number of bits set
     */
    int cardinality();

    public abstract class Factory {

        /**
         * Creates a {@link BitKey} with a capacity for a given number of bits.
         * @param size Number of bits in key
         */
        public static BitKey makeBitKey(int size) {
            return makeBitKey(size, false);
        }

        /**
         * Creates a {@link BitKey} with a capacity for a given number of bits.
         * @param size Number of bits in key
         * @param init The default value of all bits.
         */
        public static BitKey makeBitKey(int size, boolean init) {
            if (size < 0) {
                String msg = "Negative size \"" + size + "\" not allowed";
                throw new IllegalArgumentException(msg);
            }
            final BitKey bk;
            if (size < 64) {
                bk = new SmallBitKey();
            } else if (size < 128) {
                bk = new MiddleBitKey();
            } else {
                bk = new BigBitKey(size);
            }
            if (init) {
                for (int i = 0; i < size; i++) {
                    bk.set(i, init);
                }
            }
            return bk;
        }

        /**
         * Creates a {@link BitKey} with the same contents as a {@link BitSet}.
         */
        public static BitKey makeBitKey(BitSet bitSet) {
            BitKey bitKey = makeBitKey(bitSet.length());
            for (int i = bitSet.nextSetBit(0);
                i >= 0;
                i = bitSet.nextSetBit(i + 1))
            {
                bitKey.set(i);
            }
            return bitKey;
        }
    }

}
