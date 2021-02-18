import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class Settings {
  public String chunksDir;
  public int concurrentNum;
  public boolean useLrs;
  public boolean useLrsReq;
  public boolean multiNodeEncode;
  private static Settings curSettings = null;
  private static Boolean lock = false;

  public Settings(String chunksDir, int concurrentNum, boolean useLrs, boolean useLrsReq, boolean multiNodeEncode) {
    this.chunksDir = chunksDir;
    this.concurrentNum = concurrentNum;
    this.useLrs = useLrs;
    this.useLrsReq = useLrsReq;
    this.multiNodeEncode = multiNodeEncode;
  }

  public static Settings getSettings() {
    if (curSettings == null) {
      synchronized (lock) {
        if (curSettings == null) {
          curSettings = getFromFile("config/settings.ini");
        }
      }
    }
    return curSettings;
  }

  private static Settings getFromFile(String src) {
    try {
      List<String> lines = Files.readAllLines(Paths.get("config/settings.ini"));
      HashMap<String, String> map = new HashMap<String, String>();
      String[] tmp;
      String keyTmp, valueTmp;
      for (String l : lines) {
        tmp = l.split("=");
        keyTmp = tmp[0].trim();
        valueTmp = tmp[1].trim();
        map.put(keyTmp, valueTmp);
      }
      String chunksDir = map.get("chunksDir");
      int concurrentNum = map.containsKey("concurrentNum") ? Integer.parseInt(map.get("concurrentNum")) : 1;
      boolean useLrsReq = map.containsKey("useLrsRequestor") ? Boolean.parseBoolean(map.get("useLrsRequestor")) : false;
      boolean multiNodeEncode = map.containsKey("multiNodeEncode") ? Boolean.parseBoolean(map.get("multiNodeEncode"))
          : false;
      boolean useLrs = map.containsKey("useLrs") ? Boolean.parseBoolean(map.get("useLrs")) : false;
      return new Settings(chunksDir, concurrentNum, useLrs, useLrsReq, multiNodeEncode);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

}
