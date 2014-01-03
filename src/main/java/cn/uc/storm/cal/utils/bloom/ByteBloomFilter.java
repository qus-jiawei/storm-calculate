package cn.uc.storm.cal.utils.bloom;

import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Random;

/**
 * 借用了hbase的bloomfilter的实现，修改部分有：
 * 1. 取消了Hash接口，指定使用MurmurHash
 */
public class ByteBloomFilter {

  /** Bytes (B) in the array. This actually has to fit into an int. */
  protected long byteSize;
  /** Number of hash functions */
  protected int hashCount;
  /** Hash Function */
  protected final MurmurHash hash;
  /** Keys currently in the bloom */
  protected int keyCount;
  /** Max Keys expected for the bloom */
  protected int maxKeys;
  /** Bloom bits */
  protected ByteBuffer bloom;

  /** Record separator for the Bloom filter statistics human-readable string */
  public static final String STATS_RECORD_SEP = "; ";

  /**
   * Used in computing the optimal Bloom filter size. This approximately equals
   * 0.480453.
   */
  public static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  /** Bit-value lookup array to prevent doing the same work over and over */
  private static final byte [] bitvals = {
    (byte) 0x01,
    (byte) 0x02,
    (byte) 0x04,
    (byte) 0x08,
    (byte) 0x10,
    (byte) 0x20,
    (byte) 0x40,
    (byte) 0x80
  };

  /**
   * @param maxKeys
   * @param errorRate
   * @return the number of bits for a Bloom filter than can hold the given
   *         number of keys and provide the given error rate, assuming that the
   *         optimal number of hash functions is used and it does not have to
   *         be an integer.
   */
  public static long computeBitSize(long maxKeys, double errorRate) {
    return (long) Math.ceil(maxKeys * (-Math.log(errorRate) / LOG2_SQUARED));
  }

  /**
   * The maximum number of keys we can put into a Bloom filter of a certain
   * size to maintain the given error rate, assuming the number of hash
   * functions is chosen optimally and does not even have to be an integer
   * (hence the "ideal" in the function name).
   *
   * @param bitSize
   * @param errorRate
   * @return maximum number of keys that can be inserted into the Bloom filter
   * @see #computeMaxKeys(long, double, int) for a more precise estimate
   */
  public static long idealMaxKeys(long bitSize, double errorRate) {
    // The reason we need to use floor here is that otherwise we might put
    // more keys in a Bloom filter than is allowed by the target error rate.
    return (long) (bitSize * (LOG2_SQUARED / -Math.log(errorRate)));
  }

  /**
   * The maximum number of keys we can put into a Bloom filter of a certain
   * size to get the given error rate, with the given number of hash functions.
   *
   * @param bitSize
   * @param errorRate
   * @param hashCount
   * @return the maximum number of keys that can be inserted in a Bloom filter
   *         to maintain the target error rate, if the number of hash functions
   *         is provided.
   */
  public static long computeMaxKeys(long bitSize, double errorRate,
      int hashCount) {
    return (long) (-bitSize * 1.0 / hashCount *
        Math.log(1 - Math.exp(Math.log(errorRate) / hashCount)));
  }

  /**
   * Computes the error rate for this Bloom filter, taking into account the
   * actual number of hash functions and keys inserted. The return value of
   * this function changes as a Bloom filter is being populated. Used for
   * reporting the actual error rate of compound Bloom filters when writing
   * them out.
   *
   * @return error rate for this particular Bloom filter
   */
  public double actualErrorRate() {
    return actualErrorRate(keyCount, byteSize * 8, hashCount);
  }

  /**
   * Computes the actual error rate for the given number of elements, number
   * of bits, and number of hash functions. Taken directly from the
   * <a href=
   * "http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives"
   * > Wikipedia Bloom filter article</a>.
   *
   * @param maxKeys
   * @param bitSize
   * @param functionCount
   * @return the actual error rate
   */
  public static double actualErrorRate(long maxKeys, long bitSize,
      int functionCount) {
    return Math.exp(Math.log(1 - Math.exp(-functionCount * maxKeys * 1.0
        / bitSize)) * functionCount);
  }

  /**
   * Increases the given byte size of a Bloom filter until it can be folded by
   * the given factor.
   *
   * @param bitSize
   * @param foldFactor
   * @return Foldable byte size
   */
  public static int computeFoldableByteSize(long bitSize, int foldFactor) {
    long byteSizeLong = (bitSize + 7) / 8;
    int mask = (1 << foldFactor) - 1;
    if ((mask & byteSizeLong) != 0) {
      byteSizeLong >>= foldFactor;
      ++byteSizeLong;
      byteSizeLong <<= foldFactor;
    }
    if (byteSizeLong > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("byteSize=" + byteSizeLong + " too "
          + "large for bitSize=" + bitSize + ", foldFactor=" + foldFactor);
    }
    return (int) byteSizeLong;
  }

  private static int optimalFunctionCount(int maxKeys, long bitSize) {
    return (int) Math.ceil(Math.log(2) * (bitSize / maxKeys));
  }
  public ByteBloomFilter(){
	  hash = MurmurHash.getInstance();
  }
  /**
   * Determines & initializes bloom filter meta data from user config. Call
   * {@link #allocBloom()} to allocate bloom filter data.
   *
   * @param maxKeys Maximum expected number of keys that will be stored in this
   *          bloom
   * @param errorRate Desired false positive error rate. Lower rate = more
   *          storage required
   * @param hashType Type of hash function to use
   * @param foldFactor When finished adding entries, you may be able to 'fold'
   *          this bloom to save space. Tradeoff potentially excess bytes in
   *          bloom for ability to fold if keyCount is exponentially greater
   *          than maxKeys.
   * @throws IllegalArgumentException
   */
  public ByteBloomFilter(int maxKeys, double errorRate,
      int foldFactor) throws IllegalArgumentException {
	  
	hash = MurmurHash.getInstance();
    long bitSize = computeBitSize(maxKeys, errorRate);
    hashCount = optimalFunctionCount(maxKeys, bitSize);
    this.maxKeys = maxKeys;

    // increase byteSize so folding is possible
    byteSize = computeFoldableByteSize(bitSize, foldFactor);

    sanityCheck();
  }

  /**
   * Creates a Bloom filter of the given size.
   *
   * @param byteSizeHint the desired number of bytes for the Bloom filter bit
   *          array. Will be increased so that folding is possible.
   * @param errorRate target false positive rate of the Bloom filter
   * @param foldFactor
   * @return the new Bloom filter of the desired size
   */
  public static ByteBloomFilter createBySize(int byteSizeHint,
      double errorRate, int foldFactor) {
    ByteBloomFilter bbf = new ByteBloomFilter();

    bbf.byteSize = computeFoldableByteSize(byteSizeHint * 8, foldFactor);
    long bitSize = bbf.byteSize * 8;
    bbf.maxKeys = (int) idealMaxKeys(bitSize, errorRate);
    bbf.hashCount = optimalFunctionCount(bbf.maxKeys, bitSize);

    // Adjust max keys to bring error rate closer to what was requested,
    // because byteSize was adjusted to allow for folding, and hashCount was
    // rounded.
    bbf.maxKeys = (int) computeMaxKeys(bitSize, errorRate, bbf.hashCount);
    bbf.allocBloom();
    return bbf;
  }

  public void allocBloom() {
    if (this.bloom != null) {
      throw new IllegalArgumentException("can only create bloom once.");
    }
    this.bloom = ByteBuffer.allocate((int)this.byteSize);
    assert this.bloom.hasArray();
  }

  void sanityCheck() throws IllegalArgumentException {
    if(0 >= this.byteSize || this.byteSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid byteSize: " + this.byteSize);
    }

    if(this.hashCount <= 0) {
      throw new IllegalArgumentException("Hash function count must be > 0");
    }

    if (this.hash == null) {
      throw new IllegalArgumentException("hashType must be known");
    }

    if (this.keyCount < 0) {
      throw new IllegalArgumentException("must have positive keyCount");
    }
  }

  void bloomCheck(ByteBuffer bloom)  throws IllegalArgumentException {
    if (this.byteSize != bloom.limit()) {
      throw new IllegalArgumentException(
          "Configured bloom length should match actual length");
    }
  }

  public void add(byte [] buf) {
    add(buf, 0, buf.length);
  }

  public void add(byte [] buf, int offset, int len) {
    /*
     * For faster hashing, use combinatorial generation
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     */
    int hash1 = this.hash.hash(buf, offset, len, 0);
    int hash2 = this.hash.hash(buf, offset, len, hash1);

    for (int i = 0; i < this.hashCount; i++) {
      long hashLoc = Math.abs((hash1 + i * hash2) % (this.byteSize * 8));
      set(hashLoc);
    }

    ++this.keyCount;
  }

  public boolean contains(byte[] buf, int offset, int length) {
    return contains(buf, offset, length, bloom.array(),
    		bloom.arrayOffset(), (int) byteSize, hash, hashCount);
  }

  public static boolean contains(byte[] buf, int offset, int length,
      byte[] bloomArray, int bloomOffset, int bloomSize, MurmurHash hash,
      int hashCount) {

    int hash1 = hash.hash(buf, offset, length, 0);
    int hash2 = hash.hash(buf, offset, length, hash1);
    int bloomBitSize = bloomSize * 8;

      for (int i = 0; i < hashCount; i++) {
        long hashLoc = Math.abs((hash1 + i * hash2) % bloomBitSize);
        if (!get(hashLoc, bloomArray, bloomOffset))
          return false;
      }

    return true;
  }

  //---------------------------------------------------------------------------
  /** Private helpers */

  /**
   * Set the bit at the specified index to 1.
   *
   * @param pos index of bit
   */
  void set(long pos) {
    int bytePos = (int)(pos / 8);
    int bitPos = (int)(pos % 8);
    byte curByte = bloom.get(bytePos);
    curByte |= bitvals[bitPos];
    bloom.put(bytePos, curByte);
  }

  /**
   * Check if bit at specified index is 1.
   *
   * @param pos index of bit
   * @return true if bit at specified index is 1, false if 0.
   */
  static boolean get(long pos, byte[] bloomArray, int bloomOffset) {
    int bytePos = (int)(pos / 8);
    int bitPos = (int)(pos % 8);
    byte curByte = bloomArray[bloomOffset + bytePos];
    curByte &= bitvals[bitPos];
    return (curByte != 0);
  }

  public long getKeyCount() {
    return keyCount;
  }

  public long getMaxKeys() {
    return maxKeys;
  }

  public long getByteSize() {
    return byteSize;
  }

  public int getHashCount() {
    return hashCount;
  }

  /**
   * A human-readable string with statistics for the given Bloom filter.
   *
   * @param bloomFilter the Bloom filter to output statistics for;
   * @return a string consisting of "&lt;key&gt;: &lt;value&gt;" parts
   *         separated by {@link #STATS_RECORD_SEP}.
   */
  public static String formatStats(ByteBloomFilter bloomFilter) {
    StringBuilder sb = new StringBuilder();
    long k = bloomFilter.getKeyCount();
    long m = bloomFilter.getMaxKeys();

    sb.append("BloomSize: " + bloomFilter.getByteSize() + STATS_RECORD_SEP);
    sb.append("No of Keys in bloom: " + k + STATS_RECORD_SEP);
    sb.append("Max Keys for bloom: " + m);
    if (m > 0) {
      sb.append(STATS_RECORD_SEP + "Percentage filled: "
          + NumberFormat.getPercentInstance().format(k * 1.0 / m));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return formatStats(this) + STATS_RECORD_SEP + "Actual error rate: "
        + String.format("%.8f", actualErrorRate());
  }

}
