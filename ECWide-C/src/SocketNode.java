import java.io.IOException;
import java.nio.channels.SocketChannel;

public class SocketNode {
  private int nodeId;
  private SocketClient socketClient;
  private SocketServer socketServer;
  private ECTaskProcessor processor;
  private SocketChannel[] inChannels;
  private SocketChannel[] outChannels;
  private Thread clientStarter;
  private Thread serverStarter;
  private Thread processorThread;

  public SocketNode(int nodeId, ServerInfo[] targets, CodingScheme scheme, boolean isRepair) {
    this.nodeId = nodeId;
    inChannels = new SocketChannel[targets.length];
    outChannels = new SocketChannel[targets.length];
    if (nodeId != 0) {
      processor = new ECTaskProcessor(nodeId, scheme, inChannels, outChannels, isRepair);
      processorThread = new Thread(processor);
      processorThread.start();
      socketServer = new SocketServer(targets[nodeId].port, targets.length - 1, inChannels, processor.getTaskQueue());
      socketClient = new SocketClient(nodeId, targets, outChannels);
      serverStarter = new Thread(socketServer);
      serverStarter.start();
      clientStarter = new Thread(socketClient);
      clientStarter.start();
    } else {
      socketClient = new SocketClient(nodeId, targets, outChannels);
      clientStarter = new Thread(socketClient);
      clientStarter.start();
    }
  }

  public void waitConnectOK() {
    try {
      clientStarter.join();
      if (nodeId != 0) {
        serverStarter.join();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void wait4Finish() {
    try {
      processor.startServing();
      processorThread.join();
      socketClient.close();
      socketServer.close();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  public void shutdownMaster() {
    socketClient.close();
  }

  public void sendTask(int id, ECTask task) {
    try {
      socketClient.sendTask(id, task);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // args: 0-nodeID
    if (args.length != 1) {
      System.err.println("nodeID is required");
      return;
    }
    int localID = Integer.parseInt(args[0]);
    CodingScheme scheme = CodingScheme.getFromConfig("config/scheme.ini");
    ServerInfo[] infos = ServerInfo.readFromFile("config/nodes.ini");
    final boolean isRepair = true;
    SocketNode node = new SocketNode(localID, infos, scheme, isRepair);

    node.waitConnectOK();

    if (localID == 0) {
      Experiment exp = new Experiment(scheme, isRepair);
      final int expNum = 1;
      if (scheme.codeType == CodeType.CL) {
        exp.generateClTasks(expNum);
      } else if (scheme.codeType == CodeType.LRC) {
        exp.generateLrcTasks(expNum);
      } else if (scheme.codeType == CodeType.TL) {
        exp.generateTlTasks(expNum);
      }
      // exp.generateRsTasks(1);
      for (ECTask t : exp.expTasks) {
        node.sendTask(t.getNodeId(), t);
        // System.out.println(t);
      }
      node.shutdownMaster();
    } else {
      node.wait4Finish();
    }
    // System.out.println("Node finish!");
  }
}
