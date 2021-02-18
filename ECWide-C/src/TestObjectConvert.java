import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class TestObjectConvert {
  public static void main(String[] args) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream objos = new ObjectOutputStream(baos);
      objos.writeObject(new ECTask());
      byte[] outputByte = baos.toByteArray();
      int outputLen = outputByte.length;
      byteBuffer.putInt(outputLen);
      byteBuffer.put(outputByte);
      System.out.println("put in the bytebuffer: " + outputLen + " bytes");

      byteBuffer.flip();
      int inputLen = byteBuffer.getInt();
      byte[] inputByte = new byte[inputLen];
      byteBuffer.get(inputByte);
      System.out.println("get from the bytebuffer: " + inputLen + " bytes");
      ByteArrayInputStream bais = new ByteArrayInputStream(inputByte);
      ObjectInputStream objis = new ObjectInputStream(bais);
      ECTask task = (ECTask) objis.readObject();
      System.out.println("Task: " + task);
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }

  }
}
