import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileOp {
  public static void readFile(ByteBuffer b, String filename) {
    try {
      RandomAccessFile file = new RandomAccessFile(filename, "r");
      FileChannel channel = file.getChannel();
      int n, remain = b.capacity();
      b.clear();
      while (remain > 0 && (n = channel.read(b)) > 0) {
        remain -= n;
      }
      channel.close();
      file.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeFile(ByteBuffer b, String filename) {
    try {
      RandomAccessFile file = new RandomAccessFile(filename, "rw");
      FileChannel channel = file.getChannel();
      int n, remain = b.capacity();
      b.clear();
      while (remain > 0 && (n = channel.write(b)) > 0) {
        remain -= n;
      }
      channel.close();
      file.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}