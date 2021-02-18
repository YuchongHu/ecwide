import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

public class DataNodeServer implements Runnable {
  private ServerSocketChannel serverSocketChannel;
  private int listenPort;
  private int clientNum;
  private SocketChannel[] inputChannels;
  private Thread masterThread;
  private LinkedBlockingQueue<ECTask> taskQueue;

  public DataNodeServer(int port, int clientNum, SocketChannel[] inc, LinkedBlockingQueue<ECTask> taskQueue) {
    this.listenPort = port;
    this.clientNum = clientNum;
    this.inputChannels = inc;
    this.taskQueue = taskQueue;
  }

  public void runServer() throws IOException, InterruptedException {
    serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.bind(new InetSocketAddress(listenPort));

    System.out.printf("clientNum: %d\n", clientNum);
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
        System.out.println("get master");
        masterThread = new Thread(new MasterHandler(curChannel, taskQueue));
        masterThread.start();
      } else {
        System.out.printf("get client %d.\n", id);
      }
    }
    System.out.println("runServer OK!");
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
  private ByteBuffer taskBuffer;
  private LinkedBlockingQueue<ECTask> taskQueue;

  public MasterHandler(SocketChannel channel, LinkedBlockingQueue<ECTask> taskQueue) {
    this.channel = channel;
    this.taskQueue = taskQueue;
    taskBuffer = ByteBuffer.allocate(4096);
  }

  @Override
  public void run() {
    ECTask task;
    try {
      while (true) {
        task = SerializationTool.getTaskFromSocket(channel, taskBuffer);
        if (task == null) {
          System.err.println("null task");
          break;
        }
        taskQueue.add(task);
        if (task.getTaskType() == ECTaskType.SHUTDOWN) {
          // System.out.println("goodbye from master");
          break;
        }
      }
      channel.close();
    } catch (SocketException e) {
      // System.out.println("connect with master closed");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
