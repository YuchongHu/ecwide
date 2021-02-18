import java.io.IOException;

public class RequestHandler implements Runnable {
  private int nodeId;
  private NativeMessageQueue msg_queue;
  private SendWorkers sendWorkers;
  private int chunksNum;
  private boolean useLrsReq;

  public RequestHandler(int nodeId, SendWorkers sendWorkers, boolean useLrsReq) {
    this.nodeId = nodeId;
    this.sendWorkers = sendWorkers;
    this.useLrsReq = useLrsReq;
    chunksNum = 0;
    msg_queue = new NativeMessageQueue(false);
  }

  public void responseToClient() {
    // response to client
    msg_queue.makeResponse();
  }

  public void responseToClient(int resp) {
    // response to client
    msg_queue.makeResponse(resp);
  }

  public void responseErrorToClient() {
    msg_queue.makeResponse(-1);
  }

  @Override
  public void run() {
    String data, taskType, var;
    String[] splited;
    ECTask task;
    ECTaskType ecTaskType = null;
    long taskId = 0;
    boolean multiNodeEncode = Settings.getSettings().multiNodeEncode;
    while (!Thread.currentThread().isInterrupted()) {
      // wait for request from potential client
      try {
        msg_queue.waitForRequest();
      } catch (InterruptedException e) {
        break;
      }
      if (Thread.currentThread().isInterrupted()) {
        break;
      }

      data = msg_queue.getData();
      splited = data.split("#");
      if (splited.length != 2) {
        System.err.println("invalid data from msg_queue");
      }
      taskType = splited[0].trim();
      var = splited[1].trim();
      if (taskType.equals("repair_chunk")) {
        ecTaskType = ECTaskType.REPAIR_CHUNK;
      } else if (taskType.equals("repair_node")) {
        ecTaskType = ECTaskType.REPAIR_NODE;
        if (!useLrsReq) {
          msg_queue.makeResponse(chunksNum);
        }
      } else if (taskType.equals("multinode_encode")) {
        if (!multiNodeEncode) {
          System.err.println("invalid encode call");
          msg_queue.makeResponse(-1);
        }
        ecTaskType = ECTaskType.START_ENCODE;
      } else {
        System.err.println("invalid task type");
        msg_queue.makeResponse(-1);
        ecTaskType = null;
      }
      if (ecTaskType != null) {
        task = new ECTask(taskId++, ecTaskType, nodeId, 0, 0, var);
        try {
          sendWorkers.sendRequest(task);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void setChunksNum(int chunksNum) {
    this.chunksNum = chunksNum;
  }
}
