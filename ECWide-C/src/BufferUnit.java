import java.nio.ByteBuffer;

public class BufferUnit {
  ByteBuffer[] dataBuffer;
  ByteBuffer[] intermediateBuffer;
  ByteBuffer[] encodeBuffer;
  ByteBuffer[] globalPart;
  ByteBuffer decodeBuffer;

  public BufferUnit(CodingScheme scheme, boolean isRepair) {
    int i;
    if (isRepair) {
      if (scheme.codeType == CodeType.LRC) {
        dataBuffer = new ByteBuffer[scheme.groupDataNum];
        for (i = 0; i < dataBuffer.length; ++i) {
          dataBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
      } else {
        dataBuffer = new ByteBuffer[scheme.groupDataNum + scheme.groupNum - 1];
        for (i = 0; i < dataBuffer.length; ++i) {
          dataBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
      }
      decodeBuffer = ByteBuffer.allocateDirect(scheme.chunkSize);
    } else {
      if (scheme.codeType == CodeType.LRC) {
        dataBuffer = new ByteBuffer[scheme.groupDataNum * scheme.groupNum];
        for (i = 0; i < dataBuffer.length; ++i) {
          dataBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
        encodeBuffer = new ByteBuffer[scheme.globalParityNum + scheme.groupNum];
        for (i = 0; i < encodeBuffer.length; ++i) {
          encodeBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
      } else {
        dataBuffer = new ByteBuffer[scheme.groupDataNum];
        for (i = 0; i < dataBuffer.length; ++i) {
          dataBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
        if (scheme.codeType == CodeType.CL) {
          intermediateBuffer = new ByteBuffer[scheme.globalParityNum];
          for (i = 0; i < intermediateBuffer.length; ++i) {
            intermediateBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
          }
        }
        encodeBuffer = new ByteBuffer[scheme.globalParityNum + 1];
        for (i = 0; i < encodeBuffer.length; ++i) {
          encodeBuffer[i] = ByteBuffer.allocateDirect(scheme.chunkSize);
        }
        globalPart = new ByteBuffer[scheme.globalParityNum];
        for (i = 1; i < encodeBuffer.length; ++i) {
          globalPart[i - 1] = encodeBuffer[i];
        }
      }
    }
  }

  public static BufferUnit[] getMultiBuffers(CodingScheme scheme, boolean isRepair, int num) {
    BufferUnit[] b = new BufferUnit[num];
    for (int i = 0; i < num; ++i) {
      b[i] = new BufferUnit(scheme, isRepair);
    }
    return b;
  }

  public static BufferUnit getSingleBuffer(CodingScheme scheme, boolean isRepair) {
    return new BufferUnit(scheme, isRepair);
  }

  public static void clearBufferState(ByteBuffer[] b_array) {
    for (ByteBuffer b : b_array) {
      b.clear();
    }
  }
}
