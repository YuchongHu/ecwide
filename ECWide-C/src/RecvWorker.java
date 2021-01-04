import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.*;

public class RecvWorker {
  private ExecutorService encodeQueue;
  private ExecutorService repairCollectQueue;

  public RecvWorker(final CodingScheme scheme, final BufferUnit bufferUnit, SocketChannel[] in, boolean isRepair,
      int nodeId) {
    if (isRepair) {
      // int num;
      // if (scheme.codeType == CodeType.LRC) {
      // if (nodeId == 1) {
      // num = scheme.groupDataNum;
      // } else {
      // num = 1;
      // }
      // } else {
      // if (nodeId == 1) {
      // num = scheme.groupDataNum + scheme.groupNum - 1;
      // } else if (nodeId % scheme.rackNodesNum == 1) {
      // num = scheme.groupDataNum;
      // } else {
      // num = 1;
      // }
      // }
      // repairCollectQueue = Executors.newFixedThreadPool(num);
      repairCollectQueue = Executors.newSingleThreadExecutor();
    } else {
      encodeQueue = Executors.newSingleThreadExecutor();
    }
    RecvTask.setBufferUnit(bufferUnit);
    RecvTask.setscheme(scheme);
    RecvTask.setIn(in);
  }

  // method to add task into encode queue
  public Future<?> addTask(int sourceHost) {
    return encodeQueue.submit(new RecvTask(sourceHost));
  }

  // method to add task into collect queue for repair
  public void addTask(int[] senders, ArrayList<Future<?>> recvThreads) {
    // collect chunks from different nodes parallelly
    for (int i = 0; i < senders.length; ++i) {
      recvThreads.add(repairCollectQueue.submit(new RecvTask(senders[i], i)));
    }
  }

  public void close() {
    if (encodeQueue != null) {
      encodeQueue.shutdown();
    } else {
      repairCollectQueue.shutdown();
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
      if (!isRepair) {
        // recv multi intermediate results from last node
        SocketChannel curChannel = in[sourceHost];
        for (int i = 0; i < scheme.globalParityNum; ++i) {
          recvChunk(curChannel, bufferUnit.intermediateBuffer[i]);
        }
      } else {
        // test the two step time
        // Instant timePoint;
        // if (sourceHost == 2) {
        // timePoint = Instant.now();
        // System.out.println("node " + sourceHost + "start: " +
        // timePoint.toEpochMilli());
        // }

        // deal with single collect work
        recvChunk(in[sourceHost], bufferUnit.dataBuffer[collectIndex]);

        // if (scheme.codeType == CodeType.CL
        // && (sourceHost == scheme.rackNodesNum || sourceHost % scheme.rackNodesNum ==
        // 1)) {
        // timePoint = Instant.now();
        // System.out.println("node " + sourceHost + "end: " +
        // timePoint.toEpochMilli());
        // }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}