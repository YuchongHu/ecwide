import java.io.Serializable;
import java.util.Arrays;

public class ECTask implements Serializable {
  private static final long serialVersionUID = 1L;
  private long taskId;
  private ECTaskType taskType;
  private int nodeId;
  private int lastNode;
  private int nextNode;
  private int[] senders;
  private String data;

  public ECTask() {
    this(0, ECTaskType.SHUTDOWN, 0, 0, 0, null);
  }

  public ECTask(long taskId, ECTaskType taskType, int nodeId, int lastNode, int nextNode, String data) {
    this.taskId = taskId;
    this.taskType = taskType;
    this.nodeId = nodeId;
    this.lastNode = lastNode;
    this.nextNode = nextNode;
    this.data = data;
  }

  public ECTask(long taskId, ECTaskType taskType, int nodeId, int[] senders, int nextNode, String data) {
    this.taskId = taskId;
    this.taskType = taskType;
    this.nodeId = nodeId;
    this.senders = senders;
    this.nextNode = nextNode;
    this.data = data;
  }

  public int getNodeId() {
    return nodeId;
  }

  public int getLastNode() {
    return lastNode;
  }

  public int getNextNode() {
    return nextNode;
  }

  public long getTaskId() {
    return taskId;
  }

  public ECTaskType getTaskType() {
    return taskType;
  }

  public int[] getSenders() {
    return senders;
  }

  public String getData() {
    return data;
  }

  public boolean isEncodeHead() {
    return lastNode == 0;
  }

  public boolean isEncodeTail() {
    return nextNode == 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ECTask: taskId=" + taskId);
    sb.append(", taskType=" + taskType);
    sb.append(", nodeId=" + nodeId);
    if (senders == null) {
      sb.append(", lastNode=" + lastNode);
    } else {
      sb.append(", senders=" + Arrays.toString(senders));
    }
    sb.append(", nextNode=" + nextNode);
    sb.append(", data=" + data);
    return sb.toString();
  }

}

enum ECTaskType {
  ENCODE, REPAIR_RECV, REPAIR_SEND, REPAIR_RELAY, REPORT_CHUNK, REPAIR_CHUNK, REPAIR_NODE, START_ENCODE, ERROR, SHUTDOWN
}
