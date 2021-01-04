import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

public class ComputeWorker {
  private ExecutorService es;
  private NativeCodec codec;

  public ComputeWorker(CodingScheme scheme, BufferUnit buffer, boolean isRepair, int nodeId) {
    // the compute worker run in single thread, since the pipeline is unbalanced
    this.es = Executors.newSingleThreadExecutor();
    ComputeTask.setEncodeBuffer(buffer);
    if (scheme.codeType == CodeType.RS) {
      codec = new NativeCodec(scheme.k, scheme.m, scheme.chunkSize);
    } else {
      codec = new NativeCodec(scheme.groupDataNum, scheme.globalParityNum, scheme.groupNum, scheme.chunkSize);
    }
    if (isRepair) {
      // System.out.println("@@@@@ initDecodeTable start");
      codec.initDecodeTable();
      // System.out.println("@@@@@ initDecodeTable ok");
    } else {
      if (scheme.codeType == CodeType.CL) {
        codec.generateEncodeMatrix(nodeId - 1);
      } else if (scheme.codeType == CodeType.RS) {
        codec.generateEncodeMatrix();
      } else {
        // TODO: to be filled, for LRC
      }
      codec.initEncodeTable();
    }
    ComputeTask.setCodec(codec);
    // System.out.println("ComputeWorker initialize ok!");
  }

  public Future<?> addTask(ComputeType computeType) {
    return this.es.submit(new ComputeTask(computeType));
  }

  public void close() {
    es.shutdown();
  }
}

class ComputeTask implements Runnable {
  private static BufferUnit buffer;
  private static boolean localCompleted = false;
  private static NativeCodec codec;
  private ComputeType computeType;

  public ComputeTask(ComputeType computeType) {
    this.computeType = computeType;
  }

  public static void setEncodeBuffer(BufferUnit buffer) {
    ComputeTask.buffer = buffer;
  }

  public static void setCodec(NativeCodec codec) {
    ComputeTask.codec = codec;
  }

  @Override
  public void run() {
    Instant s, e;
    s = Instant.now();
    if (computeType == ComputeType.LOCAL_ENCODE) {
      BufferUnit.clearBufferState(buffer.dataBuffer);
      BufferUnit.clearBufferState(buffer.encodeBuffer);
      codec.encodeData(buffer.dataBuffer, buffer.encodeBuffer);
    } else if (computeType == ComputeType.UPDATE_INTERMEDIATE) {
      if (localCompleted) {
        BufferUnit.clearBufferState(buffer.globalPart);
        BufferUnit.clearBufferState(buffer.intermediateBuffer);
        codec.xorIntemediate(buffer.globalPart, buffer.intermediateBuffer);
      } else {
        System.err.println("Disordered Computetask!!");
      }
    } else {
      BufferUnit.clearBufferState(buffer.dataBuffer);
      buffer.decodeBuffer.clear();
      codec.decodeData(buffer.dataBuffer, buffer.decodeBuffer);
      // System.out.println("decodeData ok");
    }
    // e = Instant.now();

    localCompleted = computeType == ComputeType.LOCAL_ENCODE;
    // System.out.println("finish task " + computeType.toString() + ": " +
    // Duration.between(s, e).toMillis());
  }

}

enum ComputeType {
  LOCAL_ENCODE, UPDATE_INTERMEDIATE, DECODE;
}
