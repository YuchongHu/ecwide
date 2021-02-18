import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

public class BufferUnit {
  ByteBuffer[] dataBuffer;
  ByteBuffer[] intermediateBuffer;
  ByteBuffer[] encodeBuffer;
  private ByteBuffer[] decodeBuffer;
  private ByteBuffer[] sendBuffer;
  private ArrayBlockingQueue<ByteBuffer> sendBufferQueue;
  private ArrayBlockingQueue<ByteBuffer> decodeBufferQueue;
  private final int POOL_SIZE = 4;

  public BufferUnit(CodingScheme scheme, boolean isLocalEncode) {
    int i;
    int data_num, data_num_1 = 0, data_num_2 = 0;
    int encodeBufferNum = 0;
    Settings config = Settings.getSettings();
    if (!isLocalEncode) {
      // repair
      sendBuffer = new ByteBuffer[POOL_SIZE];
      sendBufferQueue = new ArrayBlockingQueue<ByteBuffer>(POOL_SIZE);
      for (i = 0; i < POOL_SIZE; ++i) {
        sendBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        sendBufferQueue.add(sendBuffer[i]);
      }
      decodeBuffer = new ByteBuffer[POOL_SIZE];
      decodeBufferQueue = new ArrayBlockingQueue<ByteBuffer>(POOL_SIZE);
      for (i = 0; i < POOL_SIZE; ++i) {
        decodeBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        decodeBufferQueue.add(decodeBuffer[i]);
      }
      if (scheme.codeType == CodeType.RS) {
        data_num_1 = scheme.k;
      } else if (scheme.codeType == CodeType.TL) {
        data_num_1 = scheme.rackNodesNum - 1 + scheme.rackNum - 1;
      } else if (scheme.codeType == CodeType.LRC) {
        data_num_1 = scheme.groupDataNum;
      } else if (scheme.codeType == CodeType.CL) {
        int rackPerGroup = (int) Math.ceil((scheme.groupDataNum + 1) / (double) scheme.rackNodesNum);
        data_num_1 = scheme.rackNodesNum - 1 + rackPerGroup - 1;
      }
      // ecnode
      if (scheme.codeType == CodeType.CL && config.multiNodeEncode) {
        data_num_2 = scheme.groupDataNum;
        encodeBufferNum = scheme.globalParityNum + 1;
        intermediateBuffer = new ByteBuffer[scheme.globalParityNum];
        for (i = 0; i < intermediateBuffer.length; ++i) {
          intermediateBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
      }
      data_num = Math.max(data_num_1, data_num_2);
    } else {
      data_num = scheme.k;
    }
    dataBuffer = new ByteBuffer[data_num];
    for (i = 0; i < dataBuffer.length; ++i) {
      dataBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
    }
    if (scheme.codeType == CodeType.CL || scheme.codeType == CodeType.LRC) {
      encodeBufferNum = scheme.globalParityNum + scheme.groupNum;
    } else if (encodeBufferNum == 0) {
      encodeBufferNum = scheme.globalParityNum;
    }
    encodeBuffer = new ByteBuffer[encodeBufferNum];
    for (i = 0; i < encodeBuffer.length; ++i) {
      encodeBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
    }
  }

  public static BufferUnit[] getMultiGroupBuffers(CodingScheme scheme, boolean isLocalEncode, int num) {
    BufferUnit[] b = new BufferUnit[num];
    for (int i = 0; i < num; ++i) {
      b[i] = new BufferUnit(scheme, isLocalEncode);
    }
    return b;
  }

  public static BufferUnit getSingleGroupBuffer(CodingScheme scheme, boolean isLocalEncode) {
    return new BufferUnit(scheme, isLocalEncode);
  }

  public static void clearBufferState(ByteBuffer[] b_array) {
    for (ByteBuffer b : b_array) {
      b.clear();
    }
  }

  public ByteBuffer getSendBuffer() throws InterruptedException {
    return sendBufferQueue.take();
  }

  public void returnSendBuffer(ByteBuffer buffer) {
    if (buffer != null) {
      sendBufferQueue.add(buffer);
    }
  }

  public ByteBuffer getDecodeBuffer() throws InterruptedException {
    return decodeBufferQueue.take();
  }

  public void returnDecodeBuffer(ByteBuffer buffer) {
    if (buffer != null) {
      decodeBufferQueue.add(buffer);
    }
  }
}
