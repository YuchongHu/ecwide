import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ECTaskProcessor extends Thread {
  private int nodeId;
  private ArrayBlockingQueue<ECTask> taskQueue;
  private ComputeWorker computeWorker;
  private SendWorkers sendWorkers;
  private RecvWorker recvWorker;
  private BufferUnit buffer;
  private Object startFlag;
  private long recvSimTime;
  final private double sendSimTimeUnit = 0.8192;
  final private int MAX_TASK_NUM = 8192;

  public ECTaskProcessor() {
  }

  public ECTaskProcessor(int nodeId, CodingScheme scheme, SocketChannel[] inc, SocketChannel[] outc, boolean isRepair) {
    this.nodeId = nodeId;
    startFlag = false;
    recvSimTime = (long) (sendSimTimeUnit * (scheme.chunkSize / 1024));
    taskQueue = new ArrayBlockingQueue<ECTask>(MAX_TASK_NUM);
    buffer = BufferUnit.getSingleBuffer(scheme, isRepair);
    computeWorker = new ComputeWorker(scheme, buffer, isRepair, nodeId);
    recvWorker = new RecvWorker(scheme, buffer, inc, isRepair, nodeId);
    sendWorkers = new SendWorkers(2, scheme, buffer, outc);
  }

  public void addTask(ECTask task) {
    taskQueue.add(task);
  }

  private void waitForThread(Future<?> t) {
    try {
      t.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  private void waitForThreads(ArrayList<Future<?>> threads) {
    try {
      for (Future<?> t : threads) {
        t.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    threads.clear();
  }

  public void startServing() {
    synchronized (startFlag) {
      startFlag.notify();
    }
  }

  private void close() {
    sendWorkers.close();
    recvWorker.close();
    computeWorker.close();
  }

  private void readFile(ByteBuffer b) {
    try {
      RandomAccessFile file = new RandomAccessFile("test/stdfile", "r");
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

  @Override
  public void run() {
    ArrayList<Future<?>> threads = new ArrayList<Future<?>>();
    Future<?> computeThread, recvThread, sendThread;
    boolean servingFLag = true;

    // System.out.println("waiting to start....");
    synchronized (startFlag) {
      try {
        startFlag.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    // System.out.println("now start!!");

    Instant startTime, endTime;
    Instant[] bd = new Instant[5];
    boolean[] bdFlag = new boolean[5];
    long durationTime;

    while (servingFLag) {
      try {
        ECTask task = taskQueue.take();
        // if (task.getTaskType() != ECTaskType.SHUTDOWN) {
        // System.out.println("==ECTaskProcessor get task:" + task);
        // }
        startTime = Instant.now();
        switch (task.getTaskType()) {
          case ENCODE:
            // readFile(buffer.dataBuffer[0]);
            // for (int i = 1; i < buffer.dataBuffer.length; ++i) {
            // System.arraycopy(buffer.dataBuffer[0], 0, buffer.dataBuffer[i], 0,
            // buffer.dataBuffer[0].length);
            // }
            // 1. local compute & recv delta from last one
            // if (nodeId == 1) {
            bd[0] = Instant.now();
            // }
            computeThread = computeWorker.addTask(ComputeType.LOCAL_ENCODE);
            if (!task.isEncodeHead()) {
              recvThread = recvWorker.addTask(task.getLastNode());
              waitForThread(recvThread);
              if (nodeId == 1) {
                bd[1] = Instant.now();
              }
            }
            waitForThread(computeThread);
            bd[2] = Instant.now();
            if (!task.isEncodeHead()) {
              // 2. update delta with local result
              computeThread = computeWorker.addTask(ComputeType.UPDATE_INTERMEDIATE);
              waitForThread(computeThread);
              bd[3] = Instant.now();
            }
            if (!task.isEncodeTail()) {
              // 3. send delta to next helper
              sendWorkers.addTask(task.getNextNode(), threads);
              waitForThreads(threads);
              bd[4] = Instant.now();
            }
            if (bd[1] != null) {
              durationTime = Duration.between(bd[0], bd[1]).toNanos();
              System.out.printf("recv: %f\n", durationTime / 1000000.0);
            }
            if (bd[2] != null) {
              durationTime = Duration.between(bd[0], bd[2]).toNanos();
              System.out.printf("recv & compute: %f\n", durationTime / 1000000.0);
            }
            if (bd[3] != null) {
              durationTime = Duration.between(bd[0], bd[3]).toNanos();
              System.out.printf("UPDATE_INTERMEDIATE: %f\n", durationTime / 1000000.0);
            }
            if (bd[4] != null) {
              durationTime = Duration.between(bd[0], bd[4]).toNanos();
              System.out.printf("send: %f\n", durationTime / 1000000.0);
            }
            break;

          case REPAIR_RECV:
            recvWorker.addTask(task.getSenders(), threads);
            waitForThreads(threads);
            computeThread = computeWorker.addTask(ComputeType.DECODE);
            waitForThread(computeThread);
            break;

          case REPAIR_SEND:
            readFile(buffer.dataBuffer[0]);
            sendThread = sendWorkers.addTask(task.getNextNode(), buffer.dataBuffer[0]);
            waitForThread(sendThread);
            break;

          case REPAIR_RELAY:
            int[] senders = task.getSenders();
            recvWorker.addTask(senders, threads);
            readFile(buffer.dataBuffer[senders.length]);
            waitForThreads(threads);
            computeThread = computeWorker.addTask(ComputeType.DECODE);
            waitForThread(computeThread);
            sendThread = sendWorkers.addTask(task.getNextNode(), buffer.decodeBuffer);
            waitForThread(sendThread);
            break;

          case TL_REPAIR_SEND:
            Runnable r = () -> {
              readFile(buffer.dataBuffer[0]);
            };
            Thread t = new Thread(r);
            t.start();
            TimeUnit.MICROSECONDS.sleep(recvSimTime * task.getLastNode());
            t.join();
            computeThread = computeWorker.addTask(ComputeType.DECODE);
            waitForThread(computeThread);
            sendThread = sendWorkers.addTask(task.getNextNode(), buffer.decodeBuffer);
            waitForThread(sendThread);
            break;

          case TL_REPAIR_RECV:
            recvWorker.addTask(task.getSenders(), threads);
            TimeUnit.MICROSECONDS.sleep(recvSimTime * task.getNextNode());
            waitForThreads(threads);
            computeThread = computeWorker.addTask(ComputeType.DECODE);
            waitForThread(computeThread);
            break;

          default:
            servingFLag = false;
            break;
        }
        endTime = Instant.now();
        if (task.getTaskType() != ECTaskType.SHUTDOWN) {
          durationTime = Duration.between(startTime, endTime).toNanos();
          System.out.println(durationTime / 1000000.0);
        }

      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    close();
    // System.out.println("ECTaskProcessor finished.");
  }

  public ArrayBlockingQueue<ECTask> getTaskQueue() {
    return taskQueue;
  }
}
