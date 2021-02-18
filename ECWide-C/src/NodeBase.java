import java.nio.channels.SocketChannel;

public class NodeBase {
  protected int nodeId;
  protected SocketClient socketClient;
  protected SocketChannel[] inChannels;
  protected SocketChannel[] outChannels;
  protected Thread clientStarter;
  protected Thread serverStarter;

  public NodeBase(int nodeId, ServerInfo[] targets) {
    this.nodeId = nodeId;
    inChannels = new SocketChannel[targets.length];
    outChannels = new SocketChannel[targets.length];
    socketClient = new SocketClient(nodeId, targets, outChannels);
    serverStarter = null;
    clientStarter = null;
  }

  public void waitConnectOK() {
    try {
      if (clientStarter != null && serverStarter != null) {
        clientStarter.join();
        serverStarter.join();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("waitConnectOK");
  }
}
