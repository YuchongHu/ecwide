import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class ComputeWorker {
  private ExecutorService es;
  private NativeCodec codec;

  public ComputeWorker(CodingScheme scheme, BufferUnit buffer, int nodeId) {
    // the compute worker run in single thread, since the pipeline is unbalanced
    this.es = Executors.newSingleThreadExecutor();
    ComputeTask.setEncodeBuffer(buffer);
    if (scheme.codeType == CodeType.RS) {
      codec = NativeCodec.getRsCodec(scheme);
    } else if (scheme.codeType == CodeType.TL) {
      codec = NativeCodec.getTlCodec(scheme, nodeId);
    } else if (scheme.codeType == CodeType.LRC) {
      codec = NativeCodec.getLrcCodec(scheme, nodeId);
    } else if (scheme.codeType == CodeType.CL) {
      codec = NativeCodec.getClCodec(scheme, nodeId, Settings.getSettings().multiNodeEncode);
    }
    ComputeTask.setCodec(codec);
    // System.out.println("ComputeWorker initialize ok!");
  }

  public Future<?> addTask(ComputeType computeType) {
    return this.es.submit(new ComputeTask(computeType));
  }

  public Future<?> addTask(ComputeType computeType, ByteBuffer curDecodeBuffer) {
    return this.es.submit(new ComputeTask(computeType, curDecodeBuffer));
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
  private ByteBuffer curDecodeBuffer;

  public ComputeTask(ComputeType computeType) {
    this.computeType = computeType;
  }

  public ComputeTask(ComputeType computeType, ByteBuffer curDecodeBuffer) {
    this.computeType = computeType;
    this.curDecodeBuffer = curDecodeBuffer;
  }

  public static void setEncodeBuffer(BufferUnit buffer) {
    ComputeTask.buffer = buffer;
  }

  public static void setCodec(NativeCodec codec) {
    ComputeTask.codec = codec;
  }

  @Override
  public void run() {
    // Instant s, e;
    // s = Instant.now();
    if (computeType == ComputeType.ENCODE) {
      BufferUnit.clearBufferState(buffer.dataBuffer);
      BufferUnit.clearBufferState(buffer.encodeBuffer);
      codec.encodeData(buffer.dataBuffer, buffer.encodeBuffer);

    } else if (computeType == ComputeType.UPDATE_INTERMEDIATE) {
      if (localCompleted) {
        BufferUnit.clearBufferState(buffer.encodeBuffer);
        BufferUnit.clearBufferState(buffer.intermediateBuffer);
        codec.xorIntemediate(buffer.encodeBuffer, buffer.intermediateBuffer);
      } else {
        System.err.println("Disordered Computetask!!");
      }
    } else if (computeType == ComputeType.PARTIAL_DECODE) {
      BufferUnit.clearBufferState(buffer.dataBuffer);
      curDecodeBuffer.clear();
      codec.partialDecodeData(buffer.dataBuffer, curDecodeBuffer);
    } else { // decode for reapiring
      BufferUnit.clearBufferState(buffer.dataBuffer);
      curDecodeBuffer.clear();
      codec.decodeData(buffer.dataBuffer, curDecodeBuffer);
      // System.out.println("decodeData ok");
    }
    // e = Instant.now();

    localCompleted = computeType == ComputeType.ENCODE;
    // System.out.println("finish task " + computeType.toString() + ": " +
    // Duration.between(s, e).toMillis());
  }

}

enum ComputeType {
  ENCODE, UPDATE_INTERMEDIATE, DECODE, PARTIAL_DECODE;
}
