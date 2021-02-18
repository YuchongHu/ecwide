import java.nio.ByteBuffer;

public class NativeMessageQueue {
  private boolean isClient;
  private ByteBuffer buffer;
  private int msgQueueId;

  public NativeMessageQueue(boolean isClient) {
    this.isClient = isClient;
    buffer = ByteBuffer.allocateDirect(128);
    msgQueueId = initMsgQueue();
  }

  public String getData() {
    buffer.clear();
    byte[] dst = new byte[128];
    buffer.get(dst);
    return new String(dst);
  }

  public void setData(String data) {
    setData(data.getBytes());
  }

  public void setData(byte[] data) {
    buffer.clear();
    buffer.put(data);
  }

  private native void sendMsg(int msgQueueId, boolean isReq);

  private native void recvMsg(int msgQueueId, boolean isReq);

  private native void removeMsgQueue(int msgQueueId);

  private native int initMsgQueue();

  public void waitForRequest() throws InterruptedException {
    if (isClient) {
      System.err.println("invalid call as client");
      return;
    }
    recvMsg(msgQueueId, true);
  }

  public void makeRequest(String data) {
    if (!isClient) {
      System.err.println("invalid call as request handler");
      return;
    }
    setData(data);
    sendMsg(msgQueueId, true);
  }

  public int waitForResponse() throws InterruptedException {
    if (!isClient) {
      System.err.println("invalid call as request handler");
      return -1;
    }
    recvMsg(msgQueueId, false);
    buffer.clear();
    return buffer.getInt();
  }

  public void makeResponse(int response) {
    if (isClient) {
      System.err.println("invalid call as client");
      return;
    }
    buffer.clear();
    buffer.putInt(response);
    sendMsg(msgQueueId, false);
  }

  public void makeResponse() {
    makeResponse(0);
  }

  public void close() {
    if (isClient) {
      System.err.println("invalid call as client");
      return;
    }
    removeMsgQueue(msgQueueId);
  }

  static {
    System.loadLibrary("msg_queue");
  }

}
