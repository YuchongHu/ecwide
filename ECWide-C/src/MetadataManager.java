import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

class ChunkInfo {
  public int chunkId;
  public char chunkType;
  public int nodeStoring;

  public ChunkInfo(int chunkId, char chunkType, int nodeStoring) {
    this.chunkId = chunkId;
    this.chunkType = chunkType;
    this.nodeStoring = nodeStoring;
  }
}

public abstract class MetadataManager {
  protected CodingScheme scheme;
  protected int stripeLength;
  protected int curChunkId;
  protected long curTaskId;

  protected ArrayList<ChunkInfo[]> stripeChunks;
  protected ArrayList<String> chunkIdToName;
  protected HashMap<String, Integer> chunkNameToId;
  protected ArrayList<ArrayList<Integer>> nodeChunks;

  public MetadataManager(CodingScheme scheme) {
    this.scheme = scheme;
    if (scheme.codeType == CodeType.LRC || scheme.codeType == CodeType.CL) {
      stripeLength = scheme.k + scheme.globalParityNum + scheme.groupNum;
    } else {
      stripeLength = scheme.k + scheme.m;
    }
    curChunkId = 0;
    curTaskId = 0;
    stripeChunks = new ArrayList<ChunkInfo[]>();
    chunkIdToName = new ArrayList<String>();
    chunkNameToId = new HashMap<String, Integer>();
    nodeChunks = new ArrayList<ArrayList<Integer>>(stripeLength + 1);
    for (int i = 0; i <= stripeLength; ++i) {
      nodeChunks.add(new ArrayList<Integer>());
    }
  }

  protected ParseInfo parseChunkName(String chunkName) {
    try {
      char curChunkType = Character.toUpperCase(chunkName.charAt(0));
      String[] s = chunkName.split("_");
      int curStripeId = Integer.parseInt(s[1].trim());
      int index = Integer.parseInt(s[2].trim());
      int curPos = 0;
      switch (curChunkType) {
        case 'D':
          if (scheme.codeType == CodeType.LRC || scheme.codeType == CodeType.CL) {
            curPos = index + index / scheme.groupDataNum;
          } else {
            curPos = index;
          }
          break;

        case 'L':
          if (scheme.codeType == CodeType.RS || scheme.codeType == CodeType.TL) {
            throw new Exception("no local parity in RS/TL");
          }
          if (index == scheme.groupNum - 1) {
            curPos = scheme.k + scheme.groupNum - 1;
          } else {
            curPos = index + (index + 1) * scheme.groupDataNum;
          }
          break;

        case 'G':
          if (scheme.codeType == CodeType.RS || scheme.codeType == CodeType.TL) {
            curPos = index + scheme.k;
          } else {
            curPos = index + scheme.k + scheme.groupNum;
          }
          break;

        default:
          throw new Exception("chunkType error");
      }
      return new ParseInfo(curStripeId, curChunkType, curPos);
    } catch (Exception e) {
      System.err.println("parseChunkName error");
      System.err.println(e);
      return null;
    }
  }

  public void recordChunk(String chunkName, int nodeStoring) {
    if (!chunkNameToId.containsKey(chunkName)) {
      int chunkId = curChunkId++;
      chunkIdToName.add(chunkName);
      chunkNameToId.put(chunkName, chunkId);
      nodeChunks.get(nodeStoring).add(chunkId);

      ParseInfo parseInfo = parseChunkName(chunkName);
      if (parseInfo == null) {
        return;
      }
      int needSize = parseInfo.stripeId + 1;
      if (needSize > stripeChunks.size()) {
        for (int i = stripeChunks.size(); i < needSize; ++i) {
          stripeChunks.add(new ChunkInfo[stripeLength]);
        }
      }
      stripeChunks.get(parseInfo.stripeId)[parseInfo.pos] = new ChunkInfo(chunkId, parseInfo.chunkType, nodeStoring);
    } else {
      System.err.println("existing chunk " + chunkName);
    }
  }

  public void printStripe(int stripeId) {
    ChunkInfo[] curStripe = stripeChunks.get(stripeId);
    int chunkId, node;
    String chunkName;
    for (int i = 0; i < stripeLength; ++i) {
      chunkId = curStripe[i].chunkId;
      node = curStripe[i].nodeStoring;
      chunkName = chunkIdToName.get(chunkId);
      System.out.println(chunkName + "\t" + node);
    }
  }

  protected boolean basicNodeRepair(int nodeIndex, int requestor, LinkedBlockingQueue<ECTask> tasks) {
    if (nodeIndex + 1 <= nodeChunks.size()) {
      boolean flag = false;
      LinkedBlockingQueue<ECTask> newTasks = new LinkedBlockingQueue<ECTask>();
      for (int chunkId : nodeChunks.get(nodeIndex)) {
        flag = getChunkRepairTask(chunkIdToName.get(chunkId), requestor, newTasks);
        if (!flag) {
          return false;
        }
      }
      tasks.addAll(newTasks);
      return true;
    } else {
      System.err.println("nodeIndex error in basicNodeRepair");
      return false;
    }
  }

  public abstract boolean getChunkRepairTask(String chunkName, int requestor, LinkedBlockingQueue<ECTask> tasks);

  public abstract boolean getNodeRepairTask(int nodeIndex, int requestor, LinkedBlockingQueue<ECTask> tasks);
}

class ParseInfo {
  int stripeId;
  char chunkType;
  int pos;

  public ParseInfo(int stripeId, char chunkType, int pos) {
    this.stripeId = stripeId;
    this.chunkType = chunkType;
    this.pos = pos;
  }

}