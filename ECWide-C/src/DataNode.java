import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class DataNode extends NodeBase {
  private ECTaskProcessor processor;
  private Thread processorThread;
  protected DataNodeServer dataNodeServer;
  private LinkedBlockingQueue<ECTask> taskQueue;

  public DataNode(int nodeId, ServerInfo[] targets, CodingScheme scheme) {
    super(nodeId, targets);
    taskQueue = new LinkedBlockingQueue<ECTask>();
    Settings config = Settings.getSettings();
    processor = new ECTaskProcessor(nodeId, scheme, config.chunksDir, taskQueue, inChannels, outChannels);
    processorThread = new Thread(processor);
    processorThread.start();
    dataNodeServer = new DataNodeServer(targets[nodeId].port, targets.length - 1, inChannels, taskQueue);
    serverStarter = new Thread(dataNodeServer);
    serverStarter.start();
    clientStarter = new Thread(socketClient);
    clientStarter.start();
    System.out.println("create DataNode");
  }

  public void wait4Finish() {
    try {
      processor.startServing();
      processorThread.join();
      socketClient.close();
      dataNodeServer.close();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // args: 0-nodeID
    if (args.length != 1) {
      System.err.println("nodeID is required");
      return;
    }
    int localID = Integer.parseInt(args[0].trim());
    if (localID < 0) {
      System.err.println("nodeID >= 1");
      return;
    }

    CodingScheme scheme = CodingScheme.getFromConfig("config/scheme.ini");
    ServerInfo[] infos = ServerInfo.readFromFile("config/nodes.ini");

    DataNode node = new DataNode(localID, infos, scheme);
    node.waitConnectOK();
    node.wait4Finish();
    // System.out.println("Node finish!");
  }
}
