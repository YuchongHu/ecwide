public class IntBytesConvertor {
  public static byte[] int2Bytes(int a, byte[] b) {
    b[0] = (byte) ((a >> 24) & 0xFF);
    b[1] = (byte) ((a >> 16) & 0xFF);
    b[2] = (byte) ((a >> 8) & 0xFF);
    b[3] = (byte) (a & 0xFF);
    return b;
  }

  public static int bytes2Int(byte[] b) {
    return b[3] & 0xFF | (b[2] & 0xFF) << 8 | (b[1] & 0xFF) << 16 | (b[0] & 0xFF) << 24;
  }
}
