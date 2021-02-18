import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.*;

public class RecvWorkers {
  private ExecutorService taskQueue;

  public RecvWorkers(final int recvThreadNum, final CodingScheme scheme, final BufferUnit bufferUnit,
      SocketChannel[] in, int nodeId) {
    taskQueue = Executors.newFixedThreadPool(recvThreadNum);
    RecvTask.setBufferUnit(bufferUnit);
    RecvTask.setscheme(scheme);
    RecvTask.setIn(in);
  }

  // method to add task into encode queue
  public Future<?> addTask(int sourceHost) {
    return taskQueue.submit(new RecvTask(sourceHost));
  }

  // method to add task into collect queue for repair
  public void addTask(int[] senders, ArrayList<Future<?>> recvThreads, boolean reverseFlag) {
    if (senders == null) {
      return;
    }
    // collect chunks from different nodes parallelly
    if (reverseFlag) {
      for (int i = senders.length - 1; i >= 0; --i) {
        if (senders[i] != 0) {
          recvThreads.add(taskQueue.submit(new RecvTask(senders[i], i)));
        }
      }
    } else {
      for (int i = 0; i < senders.length; ++i) {
        if (senders[i] != 0) {
          recvThreads.add(taskQueue.submit(new RecvTask(senders[i], i)));
        }
      }
    }
  }

  public void close() {
    if (taskQueue != null) {
      taskQueue.shutdown();
    }
  }
}

class RecvTask implements Runnable {
  private static SocketChannel[] in;
  private static BufferUnit bufferUnit;
  private static CodingScheme scheme;
  private int sourceHost;
  private boolean isRepair;
  private int collectIndex;

  public static void setBufferUnit(final BufferUnit bufferUnit) {
    RecvTask.bufferUnit = bufferUnit;
  }

  public static void setscheme(final CodingScheme scheme) {
    RecvTask.scheme = scheme;
  }

  public static void setIn(SocketChannel[] in) {
    RecvTask.in = in;
  }

  public RecvTask(int sourceHost) {
    this.sourceHost = sourceHost;
    this.isRepair = false;
  }

  public RecvTask(int sourceHost, int collectIndex) {
    this.sourceHost = sourceHost;
    this.isRepair = true;
    this.collectIndex = collectIndex;
  }

  public void recvChunk(SocketChannel curChannel, ByteBuffer recvBuffer) throws IOException {
    int n, remain = scheme.chunkSize;
    recvBuffer.clear();
    while (remain > 0 && (n = curChannel.read(recvBuffer)) > 0) {
      remain -= n;
    }
  }

  @Override
  public void run() {
    try {
      SocketChannel curChannel = in[sourceHost];
      if (!isRepair) {
        // recv multi intermediate results from last node
        for (int i = 0; i < scheme.globalParityNum; ++i) {
          recvChunk(curChannel, bufferUnit.intermediateBuffer[i]);
        }
      } else {
        // deal with single collect work
        recvChunk(curChannel, bufferUnit.dataBuffer[collectIndex]);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}