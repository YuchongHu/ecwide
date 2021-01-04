import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.*;

public class SendWorkers {
  private ExecutorService es;
  private CodingScheme scheme;
  private BufferUnit buffer;

  public SendWorkers(final int threadNum, final CodingScheme scheme, BufferUnit buffer, SocketChannel[] outc) {
    this.es = Executors.newFixedThreadPool(threadNum);
    this.scheme = scheme;
    this.buffer = buffer;
    SendTask.setOutc(outc);
  }

  // for encode, send several delta chunks to the next(same nodes)
  public void addTask(int next, ArrayList<Future<?>> sendThreads) {
    if (next != 0) {
      for (int i = 0; i < scheme.globalParityNum; ++i) {
        sendThreads.add(es.submit(new SendTask(next, buffer.encodeBuffer[i])));
      }
    }
  }

  // for repair, send the chunk inneed to the next, respectively(one chunk to one
  // next, multiple sending)
  public Future<?> addTask(int next, ByteBuffer sendBuffer) {
    if (next != 0) {
      return es.submit(new SendTask(next, sendBuffer));
    }
    return null;
  }

  public void close() {
    es.shutdown();
  }
}

class SendTask implements Runnable {
  private static SocketChannel[] outc;
  private final int targetHost;
  private ByteBuffer sendBuffer;

  public SendTask(int targetHost, ByteBuffer sendBuffer) {
    this.targetHost = targetHost;
    this.sendBuffer = sendBuffer;
  }

  @Override
  public void run() {
    SocketChannel curOut = outc[targetHost];
    try {
      sendBuffer.clear();
      curOut.write(sendBuffer);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void setOutc(SocketChannel[] outc) {
    SendTask.outc = outc;
  }

}