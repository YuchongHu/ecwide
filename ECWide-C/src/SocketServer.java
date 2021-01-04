import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;

public class SocketServer implements Runnable {
  private ServerSocketChannel serverSocketChannel;
  private int listenPort;
  private int clientNum;
  private SocketChannel[] inputChannels;
  private Thread masterThread;
  private ArrayBlockingQueue<ECTask> taskQueue;

  public SocketServer(int port, int clientNum, SocketChannel[] inc, ArrayBlockingQueue<ECTask> taskQueue) {
    this.listenPort = port;
    this.clientNum = clientNum;
    this.inputChannels = inc;
    this.taskQueue = taskQueue;
  }

  public void runServer() throws IOException, InterruptedException {
    serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.bind(new InetSocketAddress(listenPort));

    // System.out.printf("clientNum: %d\n", clientNum);
    ByteBuffer buffer = ByteBuffer.allocate(4);
    SocketChannel curChannel;
    for (int i = 0; i < clientNum; ++i) {
      curChannel = serverSocketChannel.accept();
      buffer.clear();
      int read_bytes = curChannel.read(buffer);
      if (read_bytes <= 0) {
        System.err.println("read index error");
        return;
      }
      int id = IntBytesConvertor.bytes2Int(buffer.array());
      inputChannels[id] = curChannel;
      if (id == 0) {
        // System.out.println("get master");
        masterThread = new Thread(new MasterHandler(curChannel, taskQueue));
        masterThread.start();
      }
      // else {
      // // System.out.printf("get client %d.\n", id);
      // }
    }
    // System.out.println("runServer OK!");
  }

  @Override
  public void run() {
    try {
      runServer();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void close() throws InterruptedException, IOException {
    masterThread.join();
    for (int i = 0; i < clientNum; ++i) {
      if (inputChannels[i] != null) {
        inputChannels[i].close();
      }
    }
    serverSocketChannel.close();
    // System.out.println("SocketServer close");
  }
}

class MasterHandler implements Runnable {
  private SocketChannel channel;
  private InputStream in;
  private ArrayBlockingQueue<ECTask> taskQueue;

  public MasterHandler(SocketChannel channel, ArrayBlockingQueue<ECTask> taskQueue) {
    this.channel = channel;
    this.taskQueue = taskQueue;
  }

  @Override
  public void run() {
    ECTask task;
    try {
      in = channel.socket().getInputStream();
      ObjectInputStream objis = new ObjectInputStream(in);
      while (true) {
        task = (ECTask) objis.readObject();
        taskQueue.add(task);
        if (task.getTaskType() == ECTaskType.SHUTDOWN) {
          // System.out.println("goodbye from master");
          break;
        }
      }

      in.close();
      channel.close();
    } catch (SocketException e) {
      // System.out.println("connect with master closed");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}

class IntBytesConvertor {
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
