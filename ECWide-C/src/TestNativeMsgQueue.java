public class TestNativeMsgQueue {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("role is required");
      return;
    }
    NativeMessageQueue msg_q;
    if (args[0].equals("s")) {
      msg_q = new NativeMessageQueue(false);
      System.out.println("init msg queue");

      try {
        for (int i = 0; i < 5; ++i) {
          msg_q.waitForRequest();
          String data = msg_q.getData();
          System.out.println("get request: " + data);

          msg_q.makeResponse(i);
          System.out.println("makeResponse");
        }
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // do nothing
      }
      msg_q.close();
      System.out.println("finish");
    } else {
      msg_q = new NativeMessageQueue(true);
      System.out.println("init msg queue");

      try {
        for (int i = 0; i < 5; ++i) {
          msg_q.makeRequest("request No." + i);
          System.out.println("makeRequest");

          Thread.sleep(500);
          int resp = msg_q.waitForResponse();
          System.out.println("get response: " + resp);
        }
      } catch (InterruptedException e) {
        // do nothing
      }
      System.out.println("finish");
    }
  }
}
