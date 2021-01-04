import java.nio.ByteBuffer;

public class NativeCodec {
  private ByteBuffer encodeMatrix;
  private ByteBuffer gftbl;
  private ByteBuffer decodeGftbl;
  private int globalNum;
  private int encodeGroupNum;
  private int chunkSize;
  private boolean isRS;
  int k;
  int m;
  int n;

  public NativeCodec(int k, int globalNum, int groupNum, int chunkSize) {
    // the k here means groupDataNum
    this.k = k;
    this.globalNum = globalNum;
    this.encodeGroupNum = groupNum;
    m = globalNum + 1; // Default: the num of local parity chunk is 1
    n = k + m;
    this.chunkSize = chunkSize;
    isRS = false;
    encodeMatrix = ByteBuffer.allocateDirect(k * m); // only store the encode matrix part
    gftbl = ByteBuffer.allocateDirect(k * m * 32);
    decodeGftbl = ByteBuffer.allocateDirect(k * 32); // decode for single chunk repair (XOR)
  }

  public NativeCodec(int k, int m, int chunkSize) {
    this.k = k;
    this.m = m;
    n = k + m;
    this.chunkSize = chunkSize;
    isRS = true;
    encodeMatrix = ByteBuffer.allocateDirect(k * m); // only store the encode matrix part
    gftbl = ByteBuffer.allocateDirect(k * m * 32);
    decodeGftbl = ByteBuffer.allocateDirect(k * 32); // decode for single chunk repair (XOR)
  }

  public byte[] getEncodeMatrix() {
    byte[] b = new byte[encodeMatrix.capacity()];
    encodeMatrix.get(b);
    return b;
  }

  public byte[] getGftbl() {
    byte[] b = new byte[gftbl.capacity()];
    gftbl.get(b);
    return b;
  }

  public byte[] getDecodeGftbl() {
    byte[] b = new byte[decodeGftbl.capacity()];
    decodeGftbl.get(b);
    return b;
  }

  public native void generateEncodeMatrix();

  public native void generateEncodeMatrix(int index);

  public native void initEncodeTable();

  public native void initDecodeTable();

  public native void encodeData(ByteBuffer[] data, ByteBuffer[] parity);

  public native void decodeData(ByteBuffer[] data, ByteBuffer target);

  public native void xorIntemediate(ByteBuffer[] source, ByteBuffer[] target);

  static {
    System.loadLibrary("codec");
  }
}
