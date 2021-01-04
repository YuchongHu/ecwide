import java.util.ArrayList;

public class Experiment {
  CodingScheme scheme;
  boolean isRepair;
  ArrayList<ECTask> expTasks;

  public Experiment(CodingScheme scheme, boolean isRepair) {
    this.scheme = scheme;
    this.isRepair = isRepair;
    this.expTasks = new ArrayList<ECTask>();
  }

  public void generateClTasks(int num) {
    if (isRepair && (scheme.groupDataNum + 1) % scheme.rackNodesNum != 0) {
      System.err.println("scheme error!");
      return;
    }
    int rackNum = (scheme.groupDataNum + 1) / scheme.rackNodesNum;
    if (isRepair) {
      // default: N_1 is the requestor
      //
      // get senders
      int senders[] = new int[scheme.rackNodesNum - 1 + rackNum - 1];
      int relayerSenders[][] = new int[rackNum][scheme.rackNodesNum - 1];
      for (int round = 1; round <= num; ++round) {
        // for inner-rack collect when rack = 0
        // for inter-rack collect when rack > 0
        for (int rack = 0; rack < rackNum; ++rack) {
          int[] s = rack == 0 ? senders : relayerSenders[rack];
          int index = 0;
          // tasks for nodes in same rack
          for (int j = 1, k = rack * scheme.rackNodesNum + 2, target = k - 1; j < scheme.rackNodesNum; ++j) {
            if (round == 1) {
              s[index++] = k;
            }
            expTasks.add(new ECTask(round, ECTaskType.REPAIR_SEND, k++, 0, target));
          }
          if (rack == 0) {
            if (round == 1) {
              // relayer nodes in other racks
              for (int i = scheme.rackNodesNum + 1; i <= scheme.groupDataNum; i += scheme.rackNodesNum) {
                senders[index++] = i;
              }
            }
            expTasks.add(new ECTask(round, ECTaskType.REPAIR_RECV, 1, s, 0)); // task for N_1
          } else {
            // task for relayer
            expTasks.add(new ECTask(round, ECTaskType.REPAIR_RELAY, 1 + rack * scheme.rackNodesNum, s, 1));
          }
        }
      }
    } else {
      for (int round = 1; round <= num; ++round) {
        for (int i = 1; i <= scheme.rackNodesNum; ++i) {
          expTasks.add(new ECTask(round, ECTaskType.ENCODE, i, i != scheme.rackNodesNum ? i + 1 : 0, i - 1));
        }
      }
    }
  }

  public void generateLrcTasks(int num) {
    if (scheme.rackNodesNum != 0) {
      System.err.println("scheme error!");
      return;
    }
    if (isRepair) {
      int[] senders = new int[scheme.groupDataNum];
      for (int taskId = 1; taskId <= num; ++taskId) {
        for (int i = 2, index = 0; i <= scheme.groupDataNum + 1; ++i) {
          expTasks.add(new ECTask(taskId, ECTaskType.REPAIR_SEND, i, 0, 1));
          senders[index++] = i;
        }
        expTasks.add(new ECTask(taskId, ECTaskType.REPAIR_RECV, 1, senders, 0));
      }
    }
  }

  public void generateRsTasks(int num) {
    if (isRepair) {
      int[] senders = new int[scheme.k];
      for (int taskId = 1; taskId <= num; ++taskId) {
        for (int i = 2, index = 0; i <= scheme.k + 1; ++i) {
          expTasks.add(new ECTask(taskId, ECTaskType.REPAIR_SEND, i, 0, 1));
          senders[index++] = i;
        }
        expTasks.add(new ECTask(taskId, ECTaskType.REPAIR_RECV, 1, senders, 0));
      }
    }
  }

  public void generateTlTasks(int num) {
    if (isRepair) {
      int[] senders = new int[scheme.groupNum - 1];
      for (int taskId = 1; taskId <= num; ++taskId) {
        int index = 0;
        for (int i = 2; i <= scheme.groupNum - 2; ++i) {
          expTasks.add(new ECTask(taskId, ECTaskType.TL_REPAIR_SEND, i, scheme.groupDataNum - 1, 1));
          senders[index++] = i;
        }
        int recvNum = scheme.k - (scheme.groupNum - 2) * scheme.groupDataNum - 1;
        expTasks.add(new ECTask(taskId, ECTaskType.TL_REPAIR_SEND, scheme.groupNum - 1, recvNum, 1));
        senders[index++] = scheme.groupNum - 1;
        expTasks.add(new ECTask(taskId, ECTaskType.TL_REPAIR_SEND, scheme.groupNum, scheme.groupDataNum - 1, 1));
        senders[index++] = scheme.groupNum;

        expTasks.add(new ECTask(taskId, ECTaskType.TL_REPAIR_RECV, 1, senders, scheme.groupDataNum - 1));
      }
    }

    // int rackNum = scheme.groupNum;
    // if (isRepair) {
    // // default: N_1 is the requestor
    // //
    // // get senders
    // int senders[] = new int[scheme.rackNodesNum - 1 + rackNum - 1];
    // int relayerSenders[][] = new int[rackNum][scheme.rackNodesNum - 1];
    // int realK = scheme.k;
    // for (int round = 1; round <= num; ++round) {
    // // for inner-rack collect when rack = 0
    // // for inter-rack collect when rack > 0
    // for (int rack = 0; rack < rackNum; ++rack) {
    // int[] s = rack == 0 ? senders : relayerSenders[rack];
    // int index = 0;
    // // tasks for nodes in same rack
    // int end, k;
    // if (rack != rackNum - 1) {
    // k = rack * scheme.rackNodesNum + 2;
    // int remain = realK - rack * scheme.rackNodesNum;
    // if (rack != rackNum - 2) {
    // end = scheme.rackNodesNum;
    // } else {
    // end = remain;
    // s = new int[remain - 1];
    // }
    // } else {
    // k = realK + 2;
    // end = scheme.globalParityNum;
    // }
    // for (int j = 1, target = k - 1; j < end; ++j) {
    // if (round == 1) {
    // s[index++] = k;
    // }
    // expTasks.add(new ECTask(round, ECTaskType.REPAIR_SEND, k++, 0, target));
    // }
    // if (rack == 0) {
    // if (round == 1) {
    // // relayer nodes in other racks
    // for (int i = scheme.rackNodesNum + 1, j = 1; j < scheme.groupNum; i +=
    // scheme.rackNodesNum, ++j) {
    // senders[index++] = i;
    // }
    // }
    // expTasks.add(new ECTask(round, ECTaskType.REPAIR_RECV, 1, s, 0)); // task for
    // N_1
    // } else {
    // // task for relayer
    // expTasks.add(new ECTask(round, ECTaskType.REPAIR_RELAY, 1 + rack *
    // scheme.rackNodesNum, s, 1));
    // }
    // }
    // }
    // }
  }

  public static void main(String[] args) {
    CodingScheme scheme = CodingScheme.getFromConfig("config/scheme.ini");
    Experiment exp = new Experiment(scheme, true);
    exp.generateClTasks(1);
    // exp.generateLrcTasks(3);
    // exp.generateRsTasks(1);
    // exp.generateTlTasks(1);
    for (ECTask t : exp.expTasks) {
      System.out.println(t);
    }
  }
}
