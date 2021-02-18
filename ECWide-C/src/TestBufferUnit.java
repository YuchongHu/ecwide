import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

public class TestBufferUnit {
  public static void main(String[] args) {
    CodingScheme scheme = CodingScheme.getFromConfig("config/scheme.ini");
    BufferUnit bufferUnit = new BufferUnit(scheme, false);

    ByteBuffer curBuffer = null;
    ArrayBlockingQueue<ByteBuffer> q = new ArrayBlockingQueue<ByteBuffer>(4);
    ByteBuffer[] old_b;
    try {
      old_b = new ByteBuffer[4];
      for (int i = 0; i < 4; ++i) {
        curBuffer = bufferUnit.getSendBuffer();
        old_b[i] = curBuffer;
        curBuffer.putInt(i);
        q.add(curBuffer);
        System.out.println("take " + i);
      }

      for (int i = 0; i < 4; ++i) {
        curBuffer = q.take();
        System.out.println(old_b[i] == curBuffer);
        curBuffer.clear();
        bufferUnit.returnSendBuffer(curBuffer);
        System.out.println("return " + i);
      }
      for (int i = 0; i < 4; ++i) {
        curBuffer = bufferUnit.getSendBuffer();
        old_b[i] = curBuffer;
        curBuffer.putInt(i);
        q.add(curBuffer);
        System.out.println("take " + i);
      }

      for (int i = 0; i < 4; ++i) {
        curBuffer = q.take();
        System.out.println(old_b[i] == curBuffer);
        curBuffer.clear();
        bufferUnit.returnSendBuffer(curBuffer);
        System.out.println("return " + i);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
