import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterServer implements Runnable {
  private ServerSocketChannel serverSocketChannel;
  private int listenPort;
  private int clientNum;
  private SocketChannel[] inputChannels;
  private LinkedBlockingQueue<ECTask> taskQueue;
  private MetadataManager manager;
  private ClMetadataManager clManager;
  private Selector selector;
  private HashMap<SocketChannel, Integer> channelToIndex;
  private ByteBuffer nodeIndexBuffer;
  private ByteBuffer taskBuffer;
  private int connected;
  private CodingScheme scheme;

  public MasterServer(int port, int clientNum, CodingScheme scheme, SocketChannel[] inc,
      LinkedBlockingQueue<ECTask> taskQueue) {
    this.listenPort = port;
    this.clientNum = clientNum;
    this.inputChannels = inc;
    this.taskQueue = taskQueue;
    this.scheme = scheme;
    connected = 0;
    channelToIndex = new HashMap<SocketChannel, Integer>();
    nodeIndexBuffer = ByteBuffer.allocate(4);
    taskBuffer = ByteBuffer.allocate(4096);
    switch (scheme.codeType) {
      case CL:
        clManager = new ClMetadataManager(scheme);
        manager = clManager;
        break;
      case TL:
        manager = new TlMetadataManager(scheme);
        break;
      case LRC:
        manager = new LrcMetadataManager(scheme);
        break;
      default:
        System.err.println("not yet..");
        return;
    }
  }

  public void runServer() throws IOException {
    boolean flag = true;
    while (flag && !Thread.currentThread().isInterrupted()) {
      try {
        selector.select();
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        Set<SelectionKey> keys = selector.selectedKeys();
        Iterator<SelectionKey> keyIterator = keys.iterator();
        while (keyIterator.hasNext()) {
          SelectionKey key = keyIterator.next();
          if (!key.isValid()) {
            continue;
          }
          if (key.isAcceptable()) {
            accept(key);
          } else if (key.isReadable()) {
            read(key);
          }
          keyIterator.remove(); // the key had been dealed
        }
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
        flag = false;
        break;
      }
    }
  }

  public void startServer() throws IOException {
    serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.configureBlocking(false);
    serverSocketChannel.bind(new InetSocketAddress(listenPort));
    selector = Selector.open();
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

    boolean flag = true;
    while (flag && connected < clientNum && !Thread.currentThread().isInterrupted()) {
      selector.select();
      Set<SelectionKey> keys = selector.selectedKeys();
      Iterator<SelectionKey> keyIterator = keys.iterator();
      while (connected < clientNum && keyIterator.hasNext()) {
        SelectionKey key = keyIterator.next();
        if (!key.isValid()) {
          continue;
        }
        try {
          if (key.isAcceptable()) {
            accept(key);
          } else if (key.isReadable()) {
            read(key);
          }
        } catch (Exception e) {
          e.printStackTrace();
          flag = false;
          break;
        }
        keyIterator.remove(); // the key had been dealed
      }
    }
  }

  private void accept(SelectionKey key) throws IOException {
    ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
    SocketChannel clientChannel = ssc.accept();
    clientChannel.configureBlocking(false);
    clientChannel.register(selector, SelectionKey.OP_READ);
    System.out.println("a new client connected " + clientChannel.getRemoteAddress());
  }

  private void read(SelectionKey key) throws IOException, ClassNotFoundException {
    SocketChannel socketChannel = (SocketChannel) key.channel();
    if (socketChannel == null) {
      return;
    }
    if (channelToIndex.containsKey(socketChannel)) { // got nodeIndex before
      int id = channelToIndex.get(socketChannel);
      ECTask task = SerializationTool.getTaskFromSocket(inputChannels[id], taskBuffer);
      if (task == null) {
        throw new IOException("null task");
      }
      ECTaskType taskType = task.getTaskType();

      System.out.println("[request]: " + taskType + ", " + task.getNodeId() + ", " + task.getData());

      boolean flag = true;
      if (taskType == ECTaskType.REPAIR_CHUNK) {
        String chunkName = task.getData();
        flag = manager.getChunkRepairTask(chunkName, id, taskQueue);
      } else if (taskType == ECTaskType.REPAIR_NODE) {
        int repairNode = Integer.parseInt(task.getData().trim());
        flag = manager.getNodeRepairTask(repairNode, id, taskQueue);
      } else if (taskType == ECTaskType.REPORT_CHUNK) {
        String[] chunks = task.getData().split("#");
        if (chunks.length >= 1) {
          for (int i = 0; i < chunks.length; ++i) {
            manager.recordChunk(chunks[i].trim(), id);
          }
        } else {
          System.err.println("Invalid REPORT_CHUNK");
        }
      } else if (taskType == ECTaskType.START_ENCODE && scheme.codeType == CodeType.CL && id == scheme.groupNum) {
        int num = Integer.parseInt(task.getData().trim());
        for (int i = 0; i < num; ++i) {
          clManager.getMultinodeEncodeTask(taskQueue);
        }
      } else {
        flag = false;
      } 
      if (flag) {
        System.out.println("deal request ok");
      } else {
        reportError(id);
        System.err.println("Invalid request");
      }
    } else {
      nodeIndexBuffer.clear();
      int read_bytes = socketChannel.read(nodeIndexBuffer);
      if (read_bytes <= 0) {
        System.err.println("read index error");
        return;
      }
      int id = IntBytesConvertor.bytes2Int(nodeIndexBuffer.array());
      inputChannels[id] = socketChannel;
      channelToIndex.put(socketChannel, id);
      System.out.println("read index " + id);
      ++connected;
      System.out.println("get node " + id);
    }
  }

  public void reportError(int node) {
    ECTask error = new ECTask(0, ECTaskType.ERROR, node, 0, 0, null);
    taskQueue.add(error);
  }

  public void close() throws InterruptedException, IOException {
    for (int i = 1; i < clientNum; ++i) {
      if (inputChannels[i] != null) {
        inputChannels[i].close();
      }
    }
    serverSocketChannel.close();
    // System.out.println("SocketServer close");
  }

  @Override
  public void run() {
    try {
      startServer();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
