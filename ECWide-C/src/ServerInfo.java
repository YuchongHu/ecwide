import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ServerInfo {
  String ip;
  int port;

  public ServerInfo(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  public ServerInfo() {
    this.ip = "127.0.0.1";
    this.port = 9799;
  }

  public static ServerInfo[] getToyExample(int num) {
    ServerInfo[] infoArray = new ServerInfo[num];
    for (int i = 0; i < num; ++i) {
      infoArray[i] = new ServerInfo("127.0.0.1", 20000 + i);
    }
    return infoArray;
  }

  public static ServerInfo[] readFromFile(String src) {
    final int port = 20000;
    ArrayList<ServerInfo> infos = new ArrayList<ServerInfo>();

    List<String> lines;
    try {
      lines = Files.readAllLines(Paths.get(src));
      if (lines.get(0).equals("127.0.0.1") || lines.get(0).equals("localhost")) {
        int num = Integer.parseInt(lines.get(1).trim());
        for (int i = 0; i < num; ++i) {
          infos.add(new ServerInfo("localhost", port + i));
        }
      } else {
        for (String l : lines) {
          infos.add(new ServerInfo(l, port));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    ServerInfo[] returnedInfo = new ServerInfo[infos.size()];
    return (ServerInfo[]) infos.toArray(returnedInfo);
  }
}
