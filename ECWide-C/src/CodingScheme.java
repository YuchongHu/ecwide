import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class CodingScheme {
  // common part
  CodeType codeType;
  int chunkSize;
  // for RS
  int k;
  int m;
  // for LRC/CL
  int groupDataNum;
  int groupNum;
  int globalParityNum;
  int rackNodesNum;

  public CodingScheme(int k, int m, int chunkSize) {
    codeType = CodeType.RS;
    this.k = k;
    this.m = m;
    this.chunkSize = chunkSize;
  }

  public CodingScheme(int groupDataNum, int groupNum, int globalParityNum, int rackNodesNum, int chunkSize,
      CodeType codeType) {
    this.groupDataNum = groupDataNum;
    this.groupNum = groupNum;
    this.globalParityNum = globalParityNum;
    this.chunkSize = chunkSize;
    this.rackNodesNum = rackNodesNum;
    // every group contains a local parity for LRC and CL
    this.codeType = codeType; // zero means no racks, LRC
  }

  public void setK(int k) {
    this.k = k;
  }

  // public static Codingscheme getToy() {
  // return new Codingscheme(2, 2, 1, 2, 1 << 10, CodeType.CL);
  // }

  public static CodingScheme getFromConfig(String src) {
    try {
      List<String> lines = Files.readAllLines(Paths.get(src));
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      CodeType codeType = CodeType.CL;
      String[] tmp;
      String keyTmp, valueTmp;
      for (String l : lines) {
        tmp = l.split("=");
        keyTmp = tmp[0].trim();
        valueTmp = tmp[1].trim();
        if (keyTmp.equals("codeType")) {
          if (valueTmp.equals("CL")) {
            codeType = CodeType.CL;
          } else if (valueTmp.equals("LRC")) {
            codeType = CodeType.LRC;
          } else if (valueTmp.equals("TL")) {
            codeType = CodeType.TL;
          } else {
            codeType = CodeType.RS;
          }
          continue;
        }
        map.put(keyTmp, Integer.parseInt(valueTmp));
      }
      final int groupDataNum = map.get("groupDataNum");
      final int groupNum = map.get("groupNum");
      final int globalParityNum = map.get("globalParityNum");
      final int rackNodesNum = map.get("rackNodesNum");
      final int chunkSizeBits = map.get("chunkSizeBits");
      final int k = map.get("k");
      final int chunkSize = 1 << chunkSizeBits;
      CodingScheme tmpscheme = new CodingScheme(groupDataNum, groupNum, globalParityNum, rackNodesNum, chunkSize,
          codeType);
      if (k != 0) {
        tmpscheme.setK(k);
      }
      return tmpscheme;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}

enum CodeType {
  RS, TL, LRC, CL
}
