// import java.util.ArrayList;
// import java.util.Collections;

// public class Main {
// private void pureSimulation() {
// final int chunkSize = 1 << 12;
// final int rackNodesNum = 4;
// final int dataNum = 64;
// final int globalParityNum = 2;
// final int groupDataNum = dataNum / rackNodesNum;

// ArrayList<Integer> encodeOrder = new ArrayList<Integer>();
// ECTaskProcessor[] nodes = new ECTaskProcessor[rackNodesNum];
// Codingscheme scheme = new Codingscheme(groupDataNum, rackNodesNum,
// globalParityNum, rackNodesNum, chunkSize);

// for (int i = 0; i < rackNodesNum; ++i) {
// encodeOrder.add(i + 1);
// nodes[i] = new ECTaskProcessor(i + 1, scheme, null, null, false);
// nodes[i].start();
// }

// for (long i = 0; i < 5; ++i) {
// Collections.shuffle(encodeOrder);
// System.out.printf("Task %d: ", i);
// for (int j = 0; j < rackNodesNum; ++j) {
// int cur = encodeOrder.get(j);
// Integer lastHelper = j != 0 ? encodeOrder.get(j - 1) : 0;
// Integer nextHelper = j < rackNodesNum - 1 ? encodeOrder.get(j + 1) : 0;
// // nodes[cur - 1].addTask(new ECTask(i, lastHelper, nextHelper));
// System.out.printf("%d<- %d ->%d, ", lastHelper, cur, nextHelper);
// }
// System.out.printf("\n");
// }

// for (int i = 0; i < rackNodesNum; ++i) {
// try {
// nodes[i].join();
// } catch (InterruptedException e) {
// e.printStackTrace();
// }
// }
// }

// public static void main(String[] args) {

// }
// }
