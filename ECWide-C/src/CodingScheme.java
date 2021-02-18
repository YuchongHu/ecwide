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
  int rackNum;
  int rackNodesNum;

  // for basic-RS/TL
  public CodingScheme(int k, int m, int chunkSize, boolean isTl) {
    codeType = isTl ? CodeType.TL : CodeType.RS;
    this.k = k;
    globalParityNum = this.m = m;
    this.chunkSize = chunkSize;
    if (isTl) {
      rackNum = (int) Math.ceil((double) k / m) + 1;
      rackNodesNum = m;
    }
  }

  // for LRC/CL
  public CodingScheme(int k, int m, int groupDataNum, int chunkSize, boolean isCl) {
    this.k = k;
    globalParityNum = this.m = m;
    this.chunkSize = chunkSize;
    this.groupDataNum = groupDataNum;
    groupNum = (int) Math.ceil((double) k / groupDataNum);
    if (isCl) {
      codeType = CodeType.CL;
      rackNodesNum = m + 1;
      rackNum = (int) Math.ceil((k + groupNum) / (double) rackNodesNum) + 1;
    } else {
      codeType = CodeType.LRC;
      rackNodesNum = rackNum = -1;
    }
  }

  public static CodingScheme getRsScheme(int k, int m, int chunkSize) {
    return new CodingScheme(k, m, chunkSize, false);
  }

  public static CodingScheme getTlScheme(int k, int m, int chunkSize) {
    return new CodingScheme(k, m, chunkSize, true);
  }

  public static CodingScheme getLrcScheme(int k, int m, int groupDataNum, int chunkSize) {
    return new CodingScheme(k, m, groupDataNum, chunkSize, false);
  }

  public static CodingScheme getClScheme(int k, int m, int groupDataNum, int chunkSize) {
    return new CodingScheme(k, m, groupDataNum, chunkSize, true);
  }

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
      final int k = map.get("k");
      final int chunkSizeBits = map.get("chunkSizeBits");
      final int chunkSize = 1 << chunkSizeBits;
      final int globalParityNum = map.get("globalParityNum");
      final int groupDataNum = codeType == CodeType.TL || codeType == CodeType.RS ? -1 : map.get("groupDataNum");
      // final int groupNum = codeType == CodeType.TL ? -1 : map.get("groupNum");
      // final int rackNodesNum = codeType == CodeType.LRC ? -1 :
      // map.get("rackNodesNum");
      // final int rackNum = codeType == CodeType.LRC ? groupNum : map.get("rackNum");
      if (codeType == CodeType.RS) {
        return CodingScheme.getRsScheme(k, globalParityNum, chunkSize);
      } else if (codeType == CodeType.TL) {
        return CodingScheme.getTlScheme(k, globalParityNum, chunkSize);
      } else if (codeType == CodeType.LRC) {
        return CodingScheme.getLrcScheme(k, globalParityNum, groupDataNum, chunkSize);
      } else {
        return CodingScheme.getClScheme(k, globalParityNum, groupDataNum, chunkSize);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}

enum CodeType {
  RS, TL, LRC, CL
}
