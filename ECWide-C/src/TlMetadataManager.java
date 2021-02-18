import java.util.concurrent.LinkedBlockingQueue;

public class TlMetadataManager extends MetadataManager {

  public TlMetadataManager(CodingScheme scheme) {
    super(scheme);
  }

  @Override
  public boolean getChunkRepairTask(String chunkName, int requestor, LinkedBlockingQueue<ECTask> tasks) {
    /*
     * Task data sender/relayer: filename of desired chunk
     * 
     * requestor: [filename of lost chunk] # [filename of local chunk] (the later
     * exists when requestor != lostChunkNode)
     * 
     */
    ParseInfo parseInfo = parseChunkName(chunkName);
    int stripeId = parseInfo.stripeId, lostChunkPos = parseInfo.pos;

    ChunkInfo[] curStripe = stripeChunks.get(stripeId);
    int lostChunkNode = curStripe[lostChunkPos].nodeStoring;
    int lostChunkRack = lostChunkPos / scheme.rackNodesNum;
    int requestorRack = requestor / scheme.rackNodesNum;
    if (requestorRack != lostChunkRack) {
      System.err.println("requestor is not in the same rack as lostChunkNode");
      return false;
    }
    int dataRackNum = scheme.rackNum - 1;
    int decodeNum = NativeCodec.getTlDecodeDataNum(scheme.k, scheme.m, lostChunkNode);
    // System.out.println("TL decodeNum = " + decodeNum);
    int[] requestorRecv = new int[requestor != lostChunkNode ? decodeNum - 1 : decodeNum];
    String requestorLocalChunk = null;
    int recvIndex = 0;
    for (int curRackIndex = 0,
        rackStartIndex = 0; curRackIndex < dataRackNum; ++curRackIndex, rackStartIndex += scheme.rackNodesNum) {
      int curRackData = curRackIndex == dataRackNum - 1 ? scheme.k - curRackIndex * scheme.rackNodesNum
          : scheme.rackNodesNum;
      int sendersNum;
      int targetNode;
      int[] innerSenders = null;
      if (curRackIndex == lostChunkRack) {
        targetNode = requestor;
        sendersNum = requestor != lostChunkNode ? Math.max(0, scheme.rackNodesNum - 2)
            : Math.max(0, scheme.rackNodesNum - 1);
      } else {
        sendersNum = curRackData - 1;
        innerSenders = new int[sendersNum];
        // Default relayer: the node storing the first chunk in this rack
        targetNode = curStripe[rackStartIndex].nodeStoring;
      }
      // add sender tasks
      String relayerChunk = null;
      int curPos = rackStartIndex;
      for (int j = 0; j < sendersNum; ++curPos) {
        int desireNode = curStripe[curPos].nodeStoring;
        if (curPos == lostChunkPos || desireNode == targetNode) {
          if (curRackIndex != lostChunkRack) {
            relayerChunk = chunkIdToName.get(curStripe[curPos].chunkId);
          } else if (lostChunkNode != requestor && desireNode == requestor) {
            requestorLocalChunk = chunkIdToName.get(curStripe[curPos].chunkId);
          }
          continue;
        }
        String desireChunk = chunkIdToName.get(curStripe[curPos].chunkId);
        if (curRackIndex == lostChunkRack) {
          requestorRecv[recvIndex++] = desireNode;
          ++j;
        } else {
          innerSenders[j++] = desireNode;
        }
        ECTask senderTask = new ECTask(curTaskId++, ECTaskType.REPAIR_SEND, desireNode, 0, targetNode, desireChunk);
        tasks.add(senderTask);
      }
      // fill requestorLocalChunk in rare case
      if (curRackIndex == lostChunkRack && requestor != lostChunkNode && requestorLocalChunk == null) {
        while (curStripe[curPos].nodeStoring != requestor) {
          ++curPos;
        }
        requestorLocalChunk = chunkIdToName.get(curStripe[curPos].chunkId);
      } else if (curRackIndex != lostChunkRack && relayerChunk == null) {
        while (curStripe[curPos].nodeStoring != targetNode) {
          ++curPos;
        }
        relayerChunk = chunkIdToName.get(curStripe[curPos].chunkId);
      }
      // add relayer task
      if (curRackIndex != lostChunkRack) {
        requestorRecv[recvIndex++] = targetNode;
        ECTask relayerTask = new ECTask(curTaskId++, ECTaskType.REPAIR_RELAY, targetNode, innerSenders, requestor,
            relayerChunk);
        tasks.add(relayerTask);
      }
    }
    // add global parity task, send the xor parity
    int desireNode = curStripe[scheme.k].nodeStoring;
    requestorRecv[recvIndex++] = desireNode;
    String xorParityName = chunkIdToName.get(curStripe[scheme.k].chunkId);
    ECTask senderTask = new ECTask(curTaskId++, ECTaskType.REPAIR_SEND, desireNode, 0, requestor, xorParityName);
    tasks.add(senderTask);
    // add requestor task
    String data = lostChunkNode != requestor ? chunkName + "#" + requestorLocalChunk : chunkName;
    ECTask requestorTask = new ECTask(curTaskId++, ECTaskType.REPAIR_RECV, requestor, requestorRecv, 0, data);
    tasks.add(requestorTask);
    return true;
  }

  @Override
  public boolean getNodeRepairTask(int nodeIndex, int requestor, LinkedBlockingQueue<ECTask> tasks) {
    // using basic node repair
    return basicNodeRepair(nodeIndex, requestor, tasks);
  }

}
