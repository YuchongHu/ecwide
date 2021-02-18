import java.util.concurrent.LinkedBlockingQueue;

class ListNode {
  public int value;
  public ListNode next;
  public ListNode last;

  public ListNode(int value) {
    this.value = value;
    next = last = null;
  }
}

class ListHolder {
  public ListNode head;
  public ListNode tail;

  public ListHolder() {
    head = tail = null;
  }

  public ListNode add(int value) {
    ListNode newNode = new ListNode(value);
    newNode.last = tail;
    if (tail != null) {
      tail.next = newNode;
    }
    tail = newNode;
    if (head == null) {
      head = newNode;
    }
    return newNode;
  }

  public void add(ListNode newNode) {
    newNode.last = tail;
    newNode.next = null;
    if (tail != null) {
      tail.next = newNode;
    }
    tail = newNode;
    if (head == null) {
      head = newNode;
    }
  }

  public ListNode pop() {
    ListNode curHead = head;
    if (head != null) {
      if (head.next != null) {
        head.next.last = null;
      } else {
        tail = null;
      }
      head = head.next;
    }
    return curHead;
  }

  public void shiftToTail(ListNode node) {
    if (tail == node) {
      return;
    }
    ListNode prevLast = node.last;
    ListNode prevNext = node.next;
    if (prevLast != null) {
      prevLast.next = prevNext;
    } else {
      head = prevNext;
    }
    if (prevNext != null) {
      prevNext.last = prevLast;
    }
    add(node);
  }

  public int popAndThenAdd() {
    if (head != null) {
      ListNode node = pop();
      add(node);
      return node.value;
    }
    return -1;
  }
}

public class ClMetadataManager extends MetadataManager {
  private boolean useLrs;
  private ListHolder[] rackLrsNodes;
  private ListNode[] nodePointers;
  private int[] nodeToRack;
  private boolean lrsRequestor;

  public ClMetadataManager(CodingScheme scheme) {
    super(scheme);
    Settings config = Settings.getSettings();
    useLrs = config.useLrs;
    lrsRequestor = useLrs && config.useLrsReq;
    int nodeNum = scheme.k + scheme.groupNum + scheme.globalParityNum;
    nodeToRack = new int[nodeNum + 1];
    if (useLrs) {
      nodePointers = new ListNode[nodeNum + 1];
      // for (int i = 0; i <= nodeNum; ++i) {
      // nodePointers[i] = null;
      // }
      rackLrsNodes = new ListHolder[scheme.rackNum];
      for (int i = 0, t = 1; i < scheme.rackNum; ++i) {
        ListHolder curRackList = new ListHolder();
        for (int j = 0; j < scheme.rackNodesNum && t <= nodeNum; ++j) {
          nodePointers[t] = curRackList.add(t);
          nodeToRack[t++] = i;
        }
        rackLrsNodes[i] = curRackList;
      }
    } else {
      for (int i = 0, t = 1; i < scheme.rackNum; ++i) {
        for (int j = 0; j < scheme.rackNodesNum && t <= nodeNum; ++j) {
          nodeToRack[t++] = i;
        }
      }
    }
  }

  private int getLrsNode(int rack) {
    ListHolder curRackList = rackLrsNodes[rack];
    int choosenNode = curRackList.popAndThenAdd();
    return choosenNode;
  }

  private void selectAndUpdateLrs(int node) {
    int rack = nodeToRack[node];
    ListHolder curRackList = rackLrsNodes[rack];
    ListNode curNode = nodePointers[node];
    curRackList.shiftToTail(curNode);
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
    int lostChunkRack = nodeToRack[lostChunkNode];
    int requestorRack = nodeToRack[requestor];
    if (lostChunkRack != requestorRack) {
      System.err.println("requestor is not in the same rack as lostChunkNode");
      return false;
    }

    // get range of desired chunks
    int curGroup;
    int t = lostChunkPos / wholeGroup;
    if (chunkType == 'D') {
      startIndex = t * wholeGroup;
      if (t == scheme.groupNum - 1) {
        endIndex = scheme.k + scheme.groupNum;
      } else {
        endIndex = startIndex + wholeGroup;
      }
      curGroup = endIndex - startIndex;
    } else if (chunkType == 'L') {
      if (t == scheme.groupNum - 1) {
        startIndex = t * wholeGroup;
      } else {
        startIndex = lostChunkPos - scheme.groupDataNum;
      }
      curGroup = lostChunkPos - startIndex + 1;
    } else {
      // not yet..
      return false;
    }

    int rackInThisGroup = (int) Math.ceil((double) curGroup / scheme.rackNodesNum);
    int decodeNum = NativeCodec.getClDecodeDataNum(scheme, lostChunkNode);
    // System.out.println("CL decodeNum = " + decodeNum);
    int[] requestorRecv = new int[requestor != lostChunkNode ? decodeNum - 1 : decodeNum];
    String requestorLocalChunk = null;
    for (int i = 0, recvIndex = 0,
        rackStartIndex = startIndex; i < rackInThisGroup; ++i, rackStartIndex += scheme.rackNodesNum) {
      int curRackData = i == rackInThisGroup - 1 ? curGroup - i * scheme.rackNodesNum : scheme.rackNodesNum;
      int sendersNum;
      int targetNode;
      int curRackIndex = nodeToRack[curStripe[rackStartIndex].nodeStoring];
      int[] innerSenders = null;
      if (curRackIndex == lostChunkRack) {
        targetNode = requestor;
        sendersNum = requestor != lostChunkNode ? Math.max(0, scheme.rackNodesNum - 2)
            : Math.max(0, scheme.rackNodesNum - 1);
      } else {
        sendersNum = curRackData - 1;
        innerSenders = new int[sendersNum];
        if (useLrs) {
          targetNode = getLrsNode(curRackIndex);
        } else {
          targetNode = curStripe[rackStartIndex].nodeStoring;
        }
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
    // add requestor task
    String data = lostChunkNode != requestor ? chunkName + "#" + requestorLocalChunk : chunkName;
    ECTask requestorTask = new ECTask(curTaskId++, ECTaskType.REPAIR_RECV, requestor, requestorRecv, 0, data);
    tasks.add(requestorTask);
    return true;
  }

  @Override
  public boolean getNodeRepairTask(int nodeIndex, int requestor, LinkedBlockingQueue<ECTask> tasks) {
    // using basic node repair
    if (!useLrs || !lrsRequestor) {
      return basicNodeRepair(nodeIndex, requestor, tasks);
    }
    if (nodeIndex + 1 > nodeChunks.size() || nodeIndex <= 0) {
      System.err.println("nodeIndex error in getNodeRepairTask");
      return false;
    }
    boolean flag = false;
    int[] requestorCount = new int[nodeChunks.size() + 1];
    int curRequestor = -1;
    int reqRack = nodeToRack[requestor];
    int rackStartIndex = requestor - (requestor - 1) % scheme.rackNodesNum;
    int rackEndIndex = rackStartIndex + scheme.rackNodesNum;
    LinkedBlockingQueue<ECTask> newTasks = new LinkedBlockingQueue<ECTask>();
    for (int chunkId : nodeChunks.get(nodeIndex)) {
      curRequestor = getLrsNode(reqRack);
      ++requestorCount[curRequestor];
      flag = getChunkRepairTask(chunkIdToName.get(chunkId), curRequestor, newTasks);
      if (!flag) {
        return false;
      }
    }
    String data;
    for (int i = rackStartIndex; i < rackEndIndex; ++i) {
      data = String.valueOf(requestorCount[i]) + "#" + String.valueOf(i == requestor);
      tasks.add(new ECTask(curTaskId++, ECTaskType.REPAIR_NODE, i, 0, 0, data));
    }
    tasks.addAll(newTasks);
    return true;
  }

  public void getMultinodeEncodeTask(LinkedBlockingQueue<ECTask> tasks) {
    for (int i = 1; i <= scheme.groupNum; ++i) {
      int lastNode = i - 1;
      int nextNode = i == scheme.groupNum ? 0 : i + 1;
      ECTask task = new ECTask(curTaskId++, ECTaskType.ENCODE, i, lastNode, nextNode, null);
      tasks.add(task);
    }
  }
}
