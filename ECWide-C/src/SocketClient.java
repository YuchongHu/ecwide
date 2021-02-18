import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class SocketClient implements Runnable {
  private int nodeId;
  private ServerInfo[] targets;
  private SocketChannel[] outc;
  private ByteBuffer taskBuffer;

  public SocketClient() {
    this.nodeId = 1;
    setTargets(new ServerInfo[] { new ServerInfo() });
  }

  public SocketClient(int nodeId, ServerInfo[] targets, SocketChannel[] outc) {
    this.nodeId = nodeId;
    this.outc = outc;
    taskBuffer = ByteBuffer.allocate(4096);
    setTargets(targets);
  }

  public void setTargets(ServerInfo[] targets) {
    this.targets = targets;
  }

  private void connetTargets() throws IOException, InterruptedException {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(nodeId);
    for (int i = 0; i < targets.length; ++i) {
      if (i == nodeId) {
        continue;
      }
      // boolean print_flag = true;
      while (true) {
        try {
          outc[i] = SocketChannel.open();
          outc[i].connect(new InetSocketAddress(targets[i].ip, targets[i].port));
        } catch (SocketException e) {
          // if (print_flag) {
          // System.out.printf("trying to reconnect node %d ...\n", i);
          // }
          // print_flag = false;
          TimeUnit.MILLISECONDS.sleep(1);
        }
        if (outc[i] != null && outc[i].isConnected()) {
          System.out.println("connect to node" + i + " OK");
          break;
        }
      }
      buffer.clear();
      outc[i].write(buffer);
    }
    System.out.println("connect all OK");
  }

  public void sendTask(int id, ECTask task) throws IOException {
    SerializationTool.sendTaskToSocket(outc[id], taskBuffer, task);
  }

  private void sayGoodBye() throws IOException {
    ECTask bye = new ECTask();
    for (int i = 0; i < targets.length; ++i) {
      if (i == nodeId) {
        continue;
      }
      SerializationTool.sendTaskToSocket(outc[i], taskBuffer, bye);
    }
  }

  @Override
  public void run() {
    try {
      connetTargets();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    try {
      if (nodeId == 0) {
        sayGoodBye();
      }
      for (int i = 0; i < targets.length; ++i) {
        if (i == nodeId) {
          continue;
        }
        outc[i].close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    // System.out.println("SocketClient close");
  }
}
