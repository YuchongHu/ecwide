import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterNode extends NodeBase {
  private MasterServer masterServer;
  private Thread distributerThread;
  private TaskDistributer distributer;
  private LinkedBlockingQueue<ECTask> taskQueue;

  public MasterNode(ServerInfo[] targets, CodingScheme scheme) {
    super(0, targets);
    taskQueue = new LinkedBlockingQueue<ECTask>();
    masterServer = new MasterServer(targets[0].port, targets.length - 1, scheme, inChannels, taskQueue);
    serverStarter = new Thread(masterServer);
    serverStarter.start();
    clientStarter = new Thread(socketClient);
    clientStarter.start();
    distributer = new TaskDistributer(taskQueue);
    distributerThread = new Thread(distributer);
    distributerThread.start();
    System.out.println("create MasterNode");
  }

  private void sendTask(int id, ECTask task) throws IOException {
    socketClient.sendTask(id, task);
  }

  private class TaskDistributer implements Runnable {
    LinkedBlockingQueue<ECTask> taskQueue;

    public TaskDistributer(LinkedBlockingQueue<ECTask> taskQueue) {
      this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
      ECTask task;
      while (!Thread.currentThread().isInterrupted()) {
        try {
          task = taskQueue.take();
          sendTask(task.getNodeId(), task);
        } catch (InterruptedException e) {
          break;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void wait4Finish() {
    try {
      masterServer.runServer();
      distributerThread.interrupt();
      distributerThread.join();
      socketClient.close();
      masterServer.close();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    CodingScheme scheme = CodingScheme.getFromConfig("config/scheme.ini");
    ServerInfo[] infos = ServerInfo.readFromFile("config/nodes.ini");

    MasterNode node = new MasterNode(infos, scheme);
    node.waitConnectOK();
    node.wait4Finish();
    // System.out.println("Node finish!");
  }
}
