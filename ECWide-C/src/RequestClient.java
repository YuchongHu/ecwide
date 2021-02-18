import java.time.Duration;
import java.time.Instant;

public class RequestClient {
  private static void usage() {
    System.out.println("usage: repair_chunk [chunk_name]");
    System.out.println("       repair_node [node_index]");
    System.out.println("       multinode_encode [stripe_num]");
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      usage();
      return;
    }
    String task = args[0], var = args[1];
    if (!task.equals("repair_chunk") && !task.equals("repair_node") && !task.equals("multinode_encode")) {
      usage();
      return;
    }
    // create task request via named msg_queue
    Instant startTime, endTime;
    NativeMessageQueue msg_queue = new NativeMessageQueue(true);
    String data = task + "#" + var;
    msg_queue.makeRequest(data);
    startTime = Instant.now();

    System.out.println("make " + task + " request");

    try {
      if (task.equals("repair_chunk")) {
        int resp = msg_queue.waitForResponse();
        if (resp < 0) {
          System.err.println("error");
          return;
        }
      } else if (task.equals("repair_node")) {
        int num = msg_queue.waitForResponse();
        if (num < 0) {
          System.err.println("error");
          return;
        }
        for (int i = 0; i < num; ++i) {
          msg_queue.waitForResponse();
        }
      } else {
        int num = Integer.parseInt(var.trim());
        for (int i = 0, resp; i < num; ++i) {
          resp = msg_queue.waitForResponse();
          if (resp < 0) {
            System.err.println("call multinode_encode error\nplease make request on node[groupNum]");
            return;
          }
        }
      }
    } catch (InterruptedException e) {
      return;
    }
    endTime = Instant.now();
    long durationTime = Duration.between(startTime, endTime).toNanos();
    System.out.println((durationTime / 1000000.0) + " ms");
  }
}
