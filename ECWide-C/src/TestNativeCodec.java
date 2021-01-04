import java.nio.ByteBuffer;
import java.util.Arrays;

public class TestNativeCodec {
  public static byte[] convert(ByteBuffer buffer) {
    buffer.clear();
    byte[] b = new byte[buffer.capacity()];
    buffer.get(b);
    return b;
  }

  public static void main(String[] args) {
    NativeCodec codec = new NativeCodec(2, 2, 1, 1 << 2);
    codec.generateEncodeMatrix(0);
    // System.out.println("before:");
    // byte[] b = codec.getGftbl();
    // for (int i = 0; i < b.length; ++i) {
    // System.out.printf("%d ", b[i]);
    // if ((i + 1) % 20 == 0) {
    // System.out.printf("\n");
    // }
    // }
    // System.out.println();
    codec.initEncodeTable();
    byte[] b = codec.getEncodeMatrix();
    for (int i = 0, index = 0; i < codec.m; ++i) {
      for (int j = 0; j < codec.k; ++j) {
        System.out.printf("%d ", b[index++]);
      }
      System.out.printf("\n");
    }

    CodingScheme scheme = new CodingScheme(2, 1, 2, 3, 1 << 2, CodeType.CL);
    BufferUnit buffer = BufferUnit.getSingleBuffer(scheme, false);
    buffer.dataBuffer[0].put(new byte[] { 1, 2, 3, 4 });
    buffer.dataBuffer[1].put(new byte[] { 58, 96, 45, 12 });
    // buffer.dataBuffer[2] = ByteBuffer.wrap(new byte[] { 89, 54, 63, 11 });
    // buffer.dataBuffer[3] = ByteBuffer.wrap(new byte[] { 8, 6, 87, 50 });
    System.out.println("parity in java before:\n" + Arrays.toString(convert(buffer.encodeBuffer[0])));
    System.out.println(Arrays.toString(convert(buffer.encodeBuffer[1])));

    codec.encodeData(buffer.dataBuffer, buffer.encodeBuffer);

    System.out.println("\nparity in java after:\n" + Arrays.toString(convert(buffer.encodeBuffer[0])));
    System.out.println(Arrays.toString(convert(buffer.encodeBuffer[1])));

    codec.initDecodeTable();
    ByteBuffer[] reserve = new ByteBuffer[2];
    reserve[0] = buffer.dataBuffer[0];
    reserve[1] = buffer.encodeBuffer[0];
    codec.decodeData(reserve, buffer.intermediateBuffer[0]);
    System.out.println("\nrecover:\n" + Arrays.toString(convert(buffer.intermediateBuffer[0])));

    // System.out.println("\n\nafter:");
    // for (int i = 0; i < b.length; ++i) {
    // System.out.printf("%d ", b[i]);
    // if ((i + 1) % 20 == 0) {
    // System.out.printf("\n");
    // }
    // }
  }
}
