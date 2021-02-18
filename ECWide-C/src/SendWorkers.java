import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.*;

public class SendWorkers {
  private ExecutorService sendEs;
  private ExecutorService relayerEs;
  private CodingScheme scheme;
  private BufferUnit buffer;
  private SocketChannel[] outc;
  private ByteBuffer taskBuffer;

  public SendWorkers(final int threadNum, final CodingScheme scheme, BufferUnit buffer, SocketChannel[] outc) {
    this.sendEs = Executors.newFixedThreadPool(threadNum);
    if (scheme.codeType == CodeType.TL || scheme.codeType == CodeType.CL) {
      this.relayerEs = Executors.newSingleThreadExecutor();
    } else {
      this.relayerEs = null;
    }
    this.scheme = scheme;
    this.buffer = buffer;
    this.outc = outc;
    taskBuffer = ByteBuffer.allocate(4096);
    SendTask.setOutc(outc);
    SendTask.setBufferUnit(buffer);
  }

  // for encode, send several delta chunks to the next(same nodes)
  public void addTask(int next, ArrayList<Future<?>> sendThreads) {
    if (next != 0) {
      for (int i = 0; i < scheme.globalParityNum; ++i) {
        sendThreads.add(sendEs.submit(new SendTask(next, buffer.encodeBuffer[i], SendType.ENCODE_SEND)));
      }
    }
  }

  // for repair, send the chunk inneed to the next, respectively(one chunk to one
  // next, multiple sending)
  public Future<?> addTask(int next, ByteBuffer sendBuffer, boolean isRelayerSend) {
    if (next != 0) {
      if (isRelayerSend) {
        return relayerEs.submit(new SendTask(next, sendBuffer, SendType.RELAYER_SEND));
      } else {
        return sendEs.submit(new SendTask(next, sendBuffer, SendType.DATA_SEND));
      }
    }
    return null;
  }

  public void sendRequest(ECTask task) throws IOException {
    SerializationTool.sendTaskToSocket(outc[0], taskBuffer, task);
  }

  public void close() {
    sendEs.shutdown();
    if (relayerEs != null) {
      relayerEs.shutdown();
    }
  }
}

class SendTask implements Runnable {
  private static SocketChannel[] outc;
  private static BufferUnit bufferUnit;
  private final int targetHost;
  private ByteBuffer sendBuffer;
  private SendType sendType;

  public SendTask(int targetHost, ByteBuffer sendBuffer, SendType sendType) {
    this.targetHost = targetHost;
    this.sendBuffer = sendBuffer;
    this.sendType = sendType;
  }

  public static void setBufferUnit(BufferUnit bufferUnit) {
    SendTask.bufferUnit = bufferUnit;
  }

  @Override
  public void run() {
    Instant startTime, endTime;
    long durationTime;
    SocketChannel curOut = outc[targetHost];
    try {
      startTime = Instant.now();
      sendBuffer.clear();
      curOut.write(sendBuffer);
      endTime = Instant.now();

      durationTime = Duration.between(startTime, endTime).toNanos();
      System.out.println("-- send Task ok, " + durationTime / 1000000.0 + " ms");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (sendType == SendType.DATA_SEND) {
        bufferUnit.returnSendBuffer(sendBuffer);
      } else if (sendType == SendType.RELAYER_SEND) {
        bufferUnit.returnDecodeBuffer(sendBuffer);
      }
    }
  }

  public static void setOutc(SocketChannel[] outc) {
    SendTask.outc = outc;
  }

}

enum SendType {
  ENCODE_SEND, RELAYER_SEND, DATA_SEND
}