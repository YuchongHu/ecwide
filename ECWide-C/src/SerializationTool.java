import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SerializationTool {
  public static ECTask getTaskFromBytes(byte[] inputBytes) throws ClassNotFoundException, IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(inputBytes);
    ObjectInputStream objis = new ObjectInputStream(bais);
    ECTask task = (ECTask) objis.readObject();
    return task;
  }

  public static byte[] getBytesOfTask(ECTask task) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream objos = new ObjectOutputStream(baos);
    objos.writeObject(task);
    baos.flush();
    byte[] outputByte = baos.toByteArray();
    objos.close();
    baos.close();
    return outputByte;
  }

  public static ECTask getTaskFromSocket(SocketChannel socketChannel, ByteBuffer taskBuffer) {
    ECTask task = null;
    try {
      // get object size
      taskBuffer.limit(4);
      taskBuffer.rewind();
      socketChannel.read(taskBuffer);
      int size = IntBytesConvertor.bytes2Int(taskBuffer.array());
      // System.out.println("obj size = " + size);
      // get object bytes
      if (size <= 0) {
        System.err.println("error obj size");
        return null;
      }
      taskBuffer.limit(size);
      taskBuffer.rewind();
      socketChannel.read(taskBuffer);
      // System.out.println("get task bytes");
      // Deserialization
      task = SerializationTool.getTaskFromBytes(taskBuffer.array());
      // System.out.println(task);
    } catch (StreamCorruptedException e) {
      task = new ECTask();
    } catch (IOException | ClassNotFoundException e) {
      System.err.println("error in: getTaskFromSocket");
      e.printStackTrace();
    }
    return task;
  }

  public static void sendTaskToSocket(SocketChannel socketChannel, ByteBuffer taskBuffer, ECTask task)
      throws IOException {
    byte[] taskBytes = SerializationTool.getBytesOfTask(task);
    // put object size (4 bytes)
    taskBuffer.clear();
    taskBuffer.putInt(taskBytes.length);
    // put object bytes
    taskBuffer.put(taskBytes);
    taskBuffer.flip();
    // send
    socketChannel.write(taskBuffer);
  }
}
