import java.util.concurrent.LinkedBlockingQueue;

public class TestMetadataManager {
  public static void main(String[] args) {
    int nodeNum;
    LinkedBlockingQueue<ECTask> tasks = new LinkedBlockingQueue<ECTask>();
    int testNum = 10;

    /*
     * LRC test
     */
    CodingScheme lrcScheme = CodingScheme.getLrcScheme(15, 3, 4, 4);
    LrcMetadataManager lrcManager = new LrcMetadataManager(lrcScheme);
    nodeNum = lrcScheme.k + lrcScheme.globalParityNum + lrcScheme.groupNum;
    int wholeGroup = lrcScheme.groupDataNum + 1;
    for (int i = 0; i < testNum; ++i) {
      // data chunk
      for (int offset = 0, chunkNum = 0; offset < nodeNum; offset += wholeGroup) {
        for (int j = 1, curNode = j + offset; j <= lrcScheme.groupDataNum && chunkNum < lrcScheme.k; ++j, ++curNode) {
          String chunk = "D_" + i + "_" + String.valueOf(chunkNum++);
          lrcManager.recordChunk(chunk, curNode);
        }
      }
      // local parity chunk
      for (int j = 0, curNode = wholeGroup; j < lrcScheme.groupNum; ++j, curNode += wholeGroup) {
        String chunk;
        if (j == lrcScheme.groupNum - 1 && lrcScheme.k % lrcScheme.groupDataNum != 0) {
          curNode = lrcScheme.groupNum + lrcScheme.k;
        }
        chunk = "L_" + i + "_" + String.valueOf(j);
        lrcManager.recordChunk(chunk, curNode);
      }
      // global parity chunk
      for (int j = 0, curNode = lrcScheme.groupNum + lrcScheme.k + 1; j < lrcScheme.globalParityNum; ++j) {
        String chunk = "G_" + i + "_" + String.valueOf(j);
        lrcManager.recordChunk(chunk, curNode++);
      }
    }
    // get repair tasks
    String chunkToRepair = "D_8_3";
    lrcManager.getChunkRepairTask(chunkToRepair, 10, tasks);
    System.out.println("\nLRC repair " + chunkToRepair + " tasks:");
    for (ECTask t : tasks) {
      // System.out.println(t.getNodeId() + " -> " + t.getNextNode() + ", " +
      // t.getTaskType() + ", " + t.getData());
      System.out.println(t);
    }
    tasks.clear();
    System.out.println("done");

    /*
     * CL test
     */
    CodingScheme clScheme = CodingScheme.getClScheme(16, 3, 7, 4);
    ClMetadataManager clManager = new ClMetadataManager(clScheme);
    nodeNum = clScheme.k + clScheme.globalParityNum + clScheme.groupNum;
    wholeGroup = clScheme.groupDataNum + 1;
    for (int i = 0; i < testNum; ++i) {
      // data chunk
      for (int offset = 0, chunkNum = 0; offset < nodeNum; offset += wholeGroup) {
        for (int j = 1, curNode = j + offset; j <= clScheme.groupDataNum && chunkNum < clScheme.k; ++j, ++curNode) {
          String chunk = "D_" + i + "_" + String.valueOf(chunkNum++);
          clManager.recordChunk(chunk, curNode);
        }
      }
      // local parity chunk
      for (int j = 0, curNode = wholeGroup; j < clScheme.groupNum; ++j, curNode += wholeGroup) {
        String chunk;
        if (j == clScheme.groupNum - 1 && clScheme.k % clScheme.groupDataNum != 0) {
          curNode = clScheme.groupNum + clScheme.k;
        }
        chunk = "L_" + i + "_" + String.valueOf(j);
        clManager.recordChunk(chunk, curNode);
      }
      // global parity chunk
      for (int j = 0, curNode = clScheme.groupNum + clScheme.k + 1; j < clScheme.globalParityNum; ++j) {
        String chunk = "G_" + i + "_" + String.valueOf(j);
        clManager.recordChunk(chunk, curNode++);
      }
    }
    // clManager.printStripe(4);
    // get repair tasks
    chunkToRepair = "D_3_0";
    clManager.getChunkRepairTask("D_0_0", 1, tasks);
    clManager.getChunkRepairTask("D_1_0", 1, tasks);
    clManager.getChunkRepairTask("D_2_0", 1, tasks);
    tasks.clear();
    clManager.getChunkRepairTask(chunkToRepair, 1, tasks);
    System.out.println("\nCL repair " + chunkToRepair + " tasks:");
    for (ECTask t : tasks) {
      // System.out.println(t.getNodeId() + " -> " + t.getNextNode() + ", " +
      // t.getTaskType() + ", " + t.getData());
      System.out.println(t);
    }
    tasks.clear();
    System.out.println("done");

    chunkToRepair = "L_4_1";
    clManager.getChunkRepairTask(chunkToRepair, 13, tasks);
    System.out.println("\nCL repair " + chunkToRepair + " tasks:");
    for (ECTask t : tasks) {
      // System.out.println(t.getNodeId() + " -> " + t.getNextNode() + ", " +
      // t.getTaskType() + ", " + t.getData());
      System.out.println(t);
    }
    tasks.clear();
    System.out.println("done");

    /*
     * TL test
     */
    CodingScheme tlScheme = CodingScheme.getTlScheme(16, 4, 4);
    TlMetadataManager tlManager = new TlMetadataManager(tlScheme);
    nodeNum = tlScheme.k + tlScheme.m;
    for (int i = 0; i < testNum; ++i) {
      // data chunk
      for (int j = 0; j < tlScheme.k;) {
        String chunk = "D_" + i + "_" + String.valueOf(j);
        tlManager.recordChunk(chunk, ++j);
      }
      // global parity chunk
      for (int j = 0, curNode = tlScheme.k + 1; j < tlScheme.m; ++j) {
        String chunk = "G_" + i + "_" + String.valueOf(j);
        tlManager.recordChunk(chunk, curNode++);
      }
    }
    chunkToRepair = "D_2_14";
    tlManager.getChunkRepairTask(chunkToRepair, 15, tasks);
    System.out.println("\nTL repair " + chunkToRepair + " tasks:");
    for (ECTask t : tasks) {
      // System.out.println(t.getNodeId() + " -> " + t.getNextNode() + ", " +
      // t.getTaskType() + ", " + t.getData());
      System.out.println(t);
    }
    System.out.println("done");

  }
}
