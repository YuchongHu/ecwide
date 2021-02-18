import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

public class ECTaskProcessor implements Runnable {
  private volatile Object startFlag;
  private int nodeId;
  private LinkedBlockingQueue<ECTask> taskQueue;
  private ComputeWorker computeWorker;
  private SendWorkers sendWorkers;
  private RecvWorkers recvWorkers;
  private BufferUnit buffer;
  private String chunksDir;
  private ArrayList<String> chunks;
  private RequestHandler requestHandler;
  private int encodeDataNum;
  private TaskWaiter taskWaiter;
  private Thread waiterThread;
  private Thread reqHandlerThread;
  private boolean useLrsReq;
  private int concurrentNum;

  public ECTaskProcessor() {
  }

  public ECTaskProcessor(int nodeId, CodingScheme scheme, String chunksDir, LinkedBlockingQueue<ECTask> taskQueue,
      SocketChannel[] inc, SocketChannel[] outc) {
    this.nodeId = nodeId;
    if (chunksDir.endsWith("/")) {
      this.chunksDir = chunksDir;
    } else {
      this.chunksDir = chunksDir + "/";
    }
    Settings config = Settings.getSettings();
    concurrentNum = config.concurrentNum;
    if (scheme.codeType == CodeType.CL) {
      encodeDataNum = scheme.groupDataNum;
      useLrsReq = config.useLrs && config.useLrsReq;
    } else {
      useLrsReq = false;
    }
    startFlag = false;
    chunks = new ArrayList<String>();
    this.taskQueue = taskQueue;
    buffer = BufferUnit.getSingleGroupBuffer(scheme, false);
    computeWorker = new ComputeWorker(scheme, buffer, nodeId);
    System.out.println("concurrentNum = " + concurrentNum);
    recvWorkers = new RecvWorkers(concurrentNum, scheme, buffer, inc, nodeId);
    sendWorkers = new SendWorkers(concurrentNum, scheme, buffer, outc);
    requestHandler = new RequestHandler(nodeId, sendWorkers, useLrsReq);
    System.out.println("requestHandler");
    taskWaiter = new TaskWaiter(nodeId, scheme, computeWorker, sendWorkers, recvWorkers, buffer, this.chunksDir, chunks,
        requestHandler, encodeDataNum, startFlag, useLrsReq);
    waiterThread = new Thread(taskWaiter);
    waiterThread.start();
    reqHandlerThread = new Thread(requestHandler);
    reqHandlerThread.start();
  }

  public void addTask(ECTask task) {
    taskQueue.add(task);
  }

  private void reportChunks() throws IOException {
    System.out.println(chunksDir);
    File dir = new File(chunksDir);
    File[] files = dir.listFiles();
    if (files == null) {
      System.err.println("list chunksDir error");
      return;
    }
    chunks.clear();
    for (int i = 0; i < files.length; ++i) {
      if (files[i].isFile()) {
        String name = files[i].getName();
        chunks.add(name);
      }
    }
    String data = String.join("#", chunks);
    ECTask request = new ECTask(0, ECTaskType.REPORT_CHUNK, nodeId, 0, 0, data);
    sendWorkers.sendRequest(request);
  }

  public void startServing() throws IOException {
    reportChunks();
    requestHandler.setChunksNum(chunks.size());
    synchronized (startFlag) {
      startFlag.notifyAll();
    }
    taskWaiter.startServing();
  }

  @Override
  public void run() {
    synchronized (startFlag) {
      try {
        startFlag.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    // System.out.println("now start!!");

    boolean servingFLag = true;
    String localChunk = null;
    int[] senders = null;

    // just need to deal with the send task
    // others will be assigned to the waiter
    while (servingFLag) {
      try {
        ECTask task = taskQueue.take();
        ECTaskType taskType = task.getTaskType();
        senders = task.getSenders();
        System.out
            .println("[task]: " + taskType + ", " + (senders == null ? task.getNextNode() : Arrays.toString(senders)));
        if (taskType.equals(ECTaskType.REPAIR_SEND)) {
          ByteBuffer curBuffer = buffer.getSendBuffer();
          localChunk = task.getData();
          FileOp.readFile(curBuffer, chunksDir + localChunk);
          sendWorkers.addTask(task.getNextNode(), curBuffer, false);
        } else if (taskType.equals(ECTaskType.SHUTDOWN)) {
          servingFLag = false;
        } else if (taskType.equals(ECTaskType.ERROR)) {
          requestHandler.responseErrorToClient();
        } else if (taskType.equals(ECTaskType.REPAIR_NODE)) {
          if (useLrsReq) {
            String[] splited = task.getData().split("#");
            if (splited.length != 2) {
              System.err.println("invalid task of node repair");
              continue;
            }
            int num = Integer.parseInt(splited[0].trim());
            boolean isCaller = Boolean.parseBoolean(splited[1].trim());
            requestHandler.responseToClient(num);
            taskWaiter.notifyNodeRepair(num, isCaller);
          } else {
            System.err.println("invalid task of node repair");
          }
        } else {
          taskWaiter.addTask(task);
        }
      } catch (InterruptedException e) {
        break;
      }
    }
    // wait for waiter
    try {
      reqHandlerThread.interrupt();
      waiterThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

class TaskWaiter implements Runnable {
  private LinkedBlockingQueue<ECTask> taskQueue;
  private int nodeId;
  private CodingScheme scheme;
  private ComputeWorker computeWorker;
  private SendWorkers sendWorkers;
  private RecvWorkers recvWorkers;
  private BufferUnit buffer;
  private String chunksDir;
  private ArrayList<String> chunks;
  private RequestHandler requestHandler;
  private int encodeDataNum;
  private boolean useLrsReq;
  private LinkedBlockingDeque<Boolean> isNodeRepairCaller;
  private volatile Object startFlag;

  public TaskWaiter(int nodeId, CodingScheme scheme, ComputeWorker computeWorker, SendWorkers sendWorkers,
      RecvWorkers recvWorkers, BufferUnit buffer, String chunksDir, ArrayList<String> chunks,
      RequestHandler requestHandler, int encodeDataNum, Object startFlag, boolean useLrsReq) {
    taskQueue = new LinkedBlockingQueue<ECTask>();
    this.nodeId = nodeId;
    this.scheme = scheme;
    this.computeWorker = computeWorker;
    this.sendWorkers = sendWorkers;
    this.recvWorkers = recvWorkers;
    this.buffer = buffer;
    this.chunksDir = chunksDir;
    this.chunks = chunks;
    this.requestHandler = requestHandler;
    this.encodeDataNum = encodeDataNum;
    this.startFlag = startFlag;
    this.useLrsReq = useLrsReq;
    isNodeRepairCaller = new LinkedBlockingDeque<Boolean>();
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

  public void startServing() throws IOException {
    synchronized (startFlag) {
      startFlag.notifyAll();
    }
  }

  private void close() {
    sendWorkers.close();
    recvWorkers.close();
    computeWorker.close();
  }

  public void addTask(ECTask task) {
    taskQueue.add(task);
  }

  public void notifyNodeRepair(int num, boolean isCaller) {
    for (int i = 0; i < num; ++i) {
      isNodeRepairCaller.add(isCaller);
    }
  }

  @Override
  public void run() {
    ArrayList<Future<?>> threads = new ArrayList<Future<?>>();
    Future<?> computeThread, recvThread, sendThread;
    boolean servingFlag = true;

    Instant startTime, endTime;
    long durationTime;
    int[] senders = null;
    String[] splitName = null;
    String localChunk = null, ChunkToRepair = null;
    boolean reverseFlag = false;
    boolean rackFlag = scheme.codeType == CodeType.TL || scheme.codeType == CodeType.CL;
    int curNodeRack = -1;
    if (rackFlag) {
      curNodeRack = (nodeId - 1) / scheme.rackNodesNum;
    }
    ByteBuffer curDecodeBuffer = null;

    while (servingFlag || !taskQueue.isEmpty()) {
      try {
        ECTask task = taskQueue.take();
        ECTaskType taskType = task.getTaskType();
        startTime = Instant.now();
        switch (taskType) {
          case ENCODE:
            // 0. read chunks
            for (int i = 0; i < encodeDataNum; ++i) {
              FileOp.readFile(buffer.dataBuffer[i], chunksDir + chunks.get(i));
            }
            // 1. local compute & recv delta from last one
            computeThread = computeWorker.addTask(ComputeType.ENCODE);
            if (!task.isEncodeHead()) {
              recvThread = recvWorkers.addTask(task.getLastNode());
              waitForThread(recvThread);
            }
            waitForThread(computeThread);
            if (!task.isEncodeHead()) {
              // 2. update delta with local result
              computeThread = computeWorker.addTask(ComputeType.UPDATE_INTERMEDIATE);
              waitForThread(computeThread);
            }
            if (!task.isEncodeTail()) {
              // 3. send delta to next helper
              sendWorkers.addTask(task.getNextNode(), threads);
              waitForThreads(threads);
            } else {
              requestHandler.responseToClient();
            }
            break;

          case REPAIR_RECV:
            senders = task.getSenders();
            splitName = task.getData().split("#");
            if (splitName.length > 1) {
              localChunk = splitName[1].trim();
            } else {
              localChunk = null;
            }
            ChunkToRepair = splitName[0].trim();
            if (rackFlag) {
              reverseFlag = senders != null && senders.length > 1
                  && (senders[0] - 1) / scheme.rackNodesNum != curNodeRack;
            }
            recvWorkers.addTask(senders, threads, reverseFlag);
            if (localChunk != null) {
              FileOp.readFile(buffer.dataBuffer[senders.length], chunksDir + localChunk);
            }
            waitForThreads(threads);
            curDecodeBuffer = buffer.getDecodeBuffer();
            computeThread = computeWorker.addTask(ComputeType.DECODE, curDecodeBuffer);
            waitForThread(computeThread);
            // FileOp.writeFile(buffer.decodeBuffer, chunksDir + ChunkToRepair);
            buffer.returnDecodeBuffer(curDecodeBuffer);
            if (!useLrsReq || isNodeRepairCaller.isEmpty() || isNodeRepairCaller.take()) {
              requestHandler.responseToClient();
            }
            break;

          case REPAIR_RELAY:
            senders = task.getSenders();
            recvWorkers.addTask(senders, threads, false);
            localChunk = task.getData();
            FileOp.readFile(buffer.dataBuffer[senders.length], chunksDir + localChunk);
            waitForThreads(threads);
            curDecodeBuffer = buffer.getDecodeBuffer();
            computeThread = computeWorker.addTask(ComputeType.PARTIAL_DECODE, curDecodeBuffer);
            waitForThread(computeThread);
            sendThread = sendWorkers.addTask(task.getNextNode(), curDecodeBuffer, true);
            // waitForThread(sendThread);
            break;

          default:
            servingFlag = false;
            break;
        }
        endTime = Instant.now();
        if (task.getTaskType() != ECTaskType.SHUTDOWN) {
          durationTime = Duration.between(startTime, endTime).toNanos();
          System.out.println(durationTime / 1000000.0 + " ms");
        }

      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    close();
  }
}