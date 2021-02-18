import java.nio.ByteBuffer;

public class NativeCodec {
  private ByteBuffer encodeMatrix; // for global parity
  private ByteBuffer encodeGftbl;
  private ByteBuffer decodeGftbl;
  private ByteBuffer partialDecodeGftbl;
  private int chunkSize;
  private char codeType;
  private boolean multiNodeEncode;
  private int encodeDataNum;
  private int decodeDataNum;
  private int partialDecodeNum;
  private int globalNum;
  private int groupNum;
  private int groupDataNum;
  private int rackPerGroup;
  private int nodeIndex;

  // for basic RS
  public NativeCodec(int k, int m, int chunkSize) {
    codeType = 'R';
    decodeDataNum = encodeDataNum = k;
    globalNum = m;
    this.chunkSize = chunkSize;
    encodeMatrix = ByteBuffer.allocateDirect(k * m); // only store the encode matrix part
    encodeGftbl = ByteBuffer.allocateDirect(k * m * 32);
    decodeGftbl = ByteBuffer.allocateDirect(k * 32); // decode for single chunk repair (XOR)
    generateEncodeMatrix();
    initEncodeTable();
    initDecodeTable();
  }

  // for TL
  public NativeCodec(int k, int m, int nodeIndex, int chunkSize) {
    codeType = 'T';
    this.nodeIndex = nodeIndex;
    this.chunkSize = chunkSize;
    globalNum = m;
    encodeDataNum = k;
    int rackNum = (int) Math.ceil((double) k / m) + 1;
    partialDecodeNum = getTlPartialDecodeNum(k, m, nodeIndex);
    decodeDataNum = partialDecodeNum - 1 + rackNum - 1;
    encodeMatrix = ByteBuffer.allocateDirect(encodeDataNum * globalNum); // only store the encode matrix part
    encodeGftbl = ByteBuffer.allocateDirect(encodeDataNum * globalNum * 32);
    decodeGftbl = ByteBuffer.allocateDirect(decodeDataNum * 32); // decode for single chunk repair (XOR)
    partialDecodeGftbl = ByteBuffer.allocateDirect(partialDecodeNum * 32);
    generateEncodeMatrix();
    initEncodeTable();
    initDecodeTable();
    initPartialDecodeTable();
  }

  // for LRC
  public NativeCodec(int k, int m, int groupDataNum, int nodeIndex, int chunkSize) {
    codeType = 'L';
    this.chunkSize = chunkSize;
    globalNum = m;
    this.groupDataNum = groupDataNum;
    groupNum = (int) Math.ceil((double) k / groupDataNum);
    encodeDataNum = k;
    int groupIndex = (nodeIndex - 1) / groupDataNum;
    if (groupIndex == groupDataNum - 1) {
      decodeDataNum = (k - 1) % groupDataNum + 1;
    } else {
      decodeDataNum = groupDataNum;
    }
    encodeMatrix = ByteBuffer.allocateDirect(encodeDataNum * globalNum); // only store the encode matrix part
    encodeGftbl = ByteBuffer.allocateDirect(encodeDataNum * globalNum * 32);
    decodeGftbl = ByteBuffer.allocateDirect(decodeDataNum * 32); // decode for single chunk repair (XOR)
    generateEncodeMatrix();
    initEncodeTable();
    initDecodeTable();
  }

  // for CL
  public NativeCodec(CodingScheme scheme, int nodeIndex, boolean multiNodeEncode) {
    codeType = 'C';
    this.nodeIndex = nodeIndex;
    this.multiNodeEncode = multiNodeEncode;
    globalNum = scheme.globalParityNum;
    chunkSize = scheme.chunkSize;
    groupNum = scheme.groupNum;
    groupDataNum = scheme.groupDataNum;
    if (multiNodeEncode) {
      // mutinode encoding for CL
      if (nodeIndex == 1) {
        encodeDataNum = (scheme.k - 1) % scheme.groupDataNum + 1;
      } else {
        encodeDataNum = scheme.groupDataNum;
      }
    } else {
      // singlenode encoding for CL
      encodeDataNum = scheme.k;
    }
    partialDecodeNum = getClPartialDecodeNum(scheme, nodeIndex);
    // decode (as requestor)
    rackPerGroup = (int) Math.ceil((scheme.groupDataNum + 1) / (double) scheme.rackNodesNum);
    decodeDataNum = partialDecodeNum - 1 + rackPerGroup - 1;

    partialDecodeGftbl = ByteBuffer.allocateDirect(partialDecodeNum * 32);
    encodeMatrix = ByteBuffer.allocateDirect(encodeDataNum * globalNum); // only store the encode matrix part
    encodeGftbl = ByteBuffer.allocateDirect(encodeDataNum * globalNum * 32);
    decodeGftbl = ByteBuffer.allocateDirect(decodeDataNum * 32); // decode for single chunk repair (XOR)
    generateEncodeMatrix();
    initEncodeTable();
    initDecodeTable();
    initPartialDecodeTable();
  }

  public static NativeCodec getRsCodec(CodingScheme scheme) {
    return new NativeCodec(scheme.k, scheme.m, scheme.chunkSize);
  }

  public static NativeCodec getTlCodec(CodingScheme scheme, int nodeIndex) {
    return new NativeCodec(scheme.k, scheme.m, nodeIndex, scheme.chunkSize);
  }

  public static NativeCodec getLrcCodec(CodingScheme scheme, int nodeIndex) {
    return new NativeCodec(scheme.k, scheme.m, scheme.groupDataNum, nodeIndex, scheme.chunkSize);
  }

  public static NativeCodec getClCodec(CodingScheme scheme, int nodeIndex, boolean multiNodeEncode) {
    return new NativeCodec(scheme, nodeIndex, multiNodeEncode);
  }

  public byte[] getEncodeMatrix() {
    byte[] b = new byte[encodeMatrix.capacity()];
    encodeMatrix.get(b);
    return b;
  }

  public byte[] getEncodeGftbl() {
    byte[] b = new byte[encodeGftbl.capacity()];
    encodeGftbl.get(b);
    return b;
  }

  public byte[] getDecodeGftbl() {
    byte[] b = new byte[decodeGftbl.capacity()];
    decodeGftbl.get(b);
    return b;
  }

  public static int getLrcDecodeDataNum(int k, int m, int groupDataNum, int nodeIndex) {
    int groupIndex = (nodeIndex - 1) / groupDataNum;
    if (groupIndex == groupDataNum - 1) {
      return (k - 1) % groupDataNum + 1;
    } else {
      return groupDataNum;
    }
  }

  public static int getTlDecodeDataNum(int k, int m, int nodeIndex) {
    int rackNum = (int) Math.ceil((double) k / m) + 1;
    int partialDecodeNum = getTlPartialDecodeNum(k, m, nodeIndex);
    return partialDecodeNum - 1 + rackNum - 1;
  }

  public static int getClDecodeDataNum(CodingScheme scheme, int nodeIndex) {
    int partialDecodeNum = getClPartialDecodeNum(scheme, nodeIndex);
    // decode (as requestor)
    int rackPerGroup = (int) Math.ceil((scheme.groupDataNum + 1) / (double) scheme.rackNodesNum);
    int crossRackNum = 0;
    int lastGroup = (scheme.k - 1) % scheme.groupDataNum + 1;
    int rackIndex = (nodeIndex - 1) / scheme.rackNodesNum;
    if (rackIndex == scheme.rackNum - 2 && lastGroup != scheme.groupDataNum) {
      crossRackNum = (int) Math.ceil((lastGroup + 1) / (double) scheme.rackNodesNum) - 1;
    } else {
      crossRackNum = rackPerGroup - 1;
    }
    return partialDecodeNum - 1 + crossRackNum;
  }

  public static int getClPartialDecodeNum(CodingScheme scheme, int nodeIndex) {
    int rackIndex = (nodeIndex - 1) / scheme.rackNodesNum;
    if (rackIndex != scheme.rackNum - 2) {
      return scheme.rackNodesNum;
    } else { // the last rack in the last data group may be incomplete
      int lastDataGroup = (scheme.k - 1) % scheme.groupDataNum + 1;
      return lastDataGroup % scheme.rackNodesNum + 1;
    }
  }

  public static int getTlPartialDecodeNum(int k, int m, int nodeIndex) {
    int rackNodesNum = m;
    int rackIndex = (nodeIndex - 1) / rackNodesNum;
    int rackNum = (int) Math.ceil((double) k / m) + 1;
    if (rackIndex == rackNum - 2) {
      int last_rack = k - rackIndex * rackNodesNum;
      return (last_rack - 1) % rackNodesNum + 1;
    } else {
      return rackNodesNum;
    }
  }

  private native void generateEncodeMatrix();

  private native void initEncodeTable();

  private native void initDecodeTable();

  private native void initPartialDecodeTable();

  public native void encodeData(ByteBuffer[] data, ByteBuffer[] parity);

  public native void decodeData(ByteBuffer[] data, ByteBuffer target);

  public native void partialDecodeData(ByteBuffer[] data, ByteBuffer target);

  public native void xorIntemediate(ByteBuffer[] source, ByteBuffer[] target);

  static {
    System.loadLibrary("codec");
  }
}
