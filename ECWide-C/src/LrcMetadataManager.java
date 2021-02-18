import java.util.concurrent.LinkedBlockingQueue;

public class LrcMetadataManager extends MetadataManager {
  public LrcMetadataManager(CodingScheme scheme) {
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
    char chunkType = parseInfo.chunkType;

    int startIndex, endIndex;
    int wholeGroup = scheme.groupDataNum + 1;
    ChunkInfo[] curStripe = stripeChunks.get(stripeId);
    int lostChunkNode = curStripe[lostChunkPos].nodeStoring;
    int[] senders = null;
    String requestorLocalChunk = null;
    int sendersNum;

    int t = lostChunkPos / wholeGroup;
    if (chunkType == 'D') {
      startIndex = t * wholeGroup;
      if (t == scheme.groupNum - 1) {
        endIndex = scheme.k + scheme.groupNum;
      } else {
        endIndex = startIndex + wholeGroup;
      }
      sendersNum = endIndex - startIndex - 1;
    } else if (chunkType == 'L') {
      if (t == scheme.groupNum - 1) {
        startIndex = t * wholeGroup;
      } else {
        startIndex = lostChunkPos - scheme.groupDataNum;
      }
      endIndex = lostChunkPos;
      sendersNum = endIndex - startIndex;
    } else {
      // not yet..
      return false;
    }
    if (lostChunkNode != requestor) {
      --sendersNum;
    }
    senders = new int[sendersNum];
    int j = 0;
    for (int i = startIndex; i < endIndex; ++i) {
      if (i == lostChunkPos) {
        continue;
      }
      int desireNode = curStripe[i].nodeStoring;
      if (desireNode == requestor) {
        requestorLocalChunk = chunkIdToName.get(curStripe[i].chunkId);
        continue;
      }
      String desireChunk = chunkIdToName.get(curStripe[i].chunkId);
      senders[j++] = desireNode;
      ECTask senderTask = new ECTask(curTaskId++, ECTaskType.REPAIR_SEND, desireNode, 0, requestor, desireChunk);
      tasks.add(senderTask);
    }
    String data;
    if (lostChunkNode != requestor && requestor <= scheme.k + scheme.groupNum
        && (lostChunkNode - 1) / wholeGroup == (requestor - 1) / wholeGroup) {
      data = chunkName + "#" + requestorLocalChunk;
    } else {
      data = chunkName;
    }
    ECTask requestorTask = new ECTask(curTaskId++, ECTaskType.REPAIR_RECV, requestor, senders, 0, data);
    tasks.add(requestorTask);
    return true;
  }

  @Override
  public boolean getNodeRepairTask(int nodeIndex, int requestor, LinkedBlockingQueue<ECTask> tasks) {
    // using basic node repair
    return basicNodeRepair(nodeIndex, requestor, tasks);
  }
}
