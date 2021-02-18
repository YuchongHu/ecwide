import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

public class ChunkGenerator {
  private CodingScheme scheme;
  private NativeCodec codec;
  private BufferUnit bufferUnit;
  private String dirName;
  private String source;

  public ChunkGenerator(CodingScheme scheme, String dirName, String source) {
    this.scheme = scheme;
    this.dirName = dirName;
    this.source = source.equals("zero") ? "/dev/zero" : "/dev/urandom";
    switch (scheme.codeType) {
      case CL:
        codec = NativeCodec.getClCodec(scheme, 1, false);
        break;
      case LRC:
        codec = NativeCodec.getLrcCodec(scheme, 1);
        break;
      case TL:
        codec = NativeCodec.getTlCodec(scheme, 1);
        break;
      default:
        codec = NativeCodec.getRsCodec(scheme);
        break;
    }
    bufferUnit = BufferUnit.getSingleGroupBuffer(scheme, true);
    File dir = new File(dirName);
    if (!dir.exists()) {
      dir.mkdirs();
    }
  }

  public void encodeChunks() {
    codec.encodeData(bufferUnit.dataBuffer, bufferUnit.encodeBuffer);
  }

  public void getSourceData() {
    System.err.println("reading source data from " + source);
    for (int i = 0; i < scheme.k; ++i) {
      ByteBuffer b = bufferUnit.dataBuffer[i];
      FileOp.readFile(b, source);
    }
  }

  public void generateChunks(int stripeId) throws IOException {
    int i, t;
    String filename;
    ByteBuffer b;
    // data chunks
    for (i = 0; i < scheme.k; ++i) {
      b = bufferUnit.dataBuffer[i];
      if (stripeId >= 0) {
        filename = dirName + "/D_" + stripeId + "_" + i;
      } else {
        if (scheme.codeType == CodeType.LRC || scheme.codeType == CodeType.CL) {
          t = i / scheme.groupDataNum;
          filename = dirName + "/" + (i + 1 + t);
        } else {
          filename = dirName + "/" + (i + 1);
        }
        filename += "_D_" + i;
      }
      FileOp.writeFile(b, filename);
    }
    // global parity chunks
    for (i = 0; i < scheme.globalParityNum; ++i) {
      b = bufferUnit.encodeBuffer[i];
      if (stripeId >= 0) {
        filename = dirName + "/G_" + stripeId + "_" + i;
      } else {
        if (scheme.codeType == CodeType.LRC || scheme.codeType == CodeType.CL) {
          t = scheme.groupNum + scheme.k;
        } else {
          t = scheme.k;
        }
        filename = dirName + "/" + (i + 1 + t) + "_G_" + i;
      }
      FileOp.writeFile(b, filename);
    }
    if (scheme.codeType == CodeType.LRC || scheme.codeType == CodeType.CL) {
      int pos = scheme.globalParityNum;
      for (i = 0; i < scheme.groupNum; ++i) {
        b = bufferUnit.encodeBuffer[pos++];
        if (stripeId >= 0) {
          filename = dirName + "/L_" + stripeId + "_" + i;
        } else {
          if (i != scheme.groupNum - 1) {
            t = (scheme.groupDataNum + 1) * (i + 1);
          } else {
            t = scheme.groupNum + scheme.k;
          }
          filename = dirName + "/" + t + "_L_" + i;
        }
        FileOp.writeFile(b, filename);
      }
    }
  }

  public static void main(String[] args) {
    int genNum = -1;
    String source;
    if (args.length == 2) {
      source = args[0].trim();
      if (!args[1].equals("toy")) {
        genNum = Integer.parseInt(args[0].trim());
      }
    } else {
      System.err.println("usage: [zero/urandom] [stripes_num/toy]");
      return;
    }
    Settings config = Settings.getSettings();
    final String dirName = config.chunksDir;
    CodingScheme scheme = CodingScheme.getFromConfig("config/scheme.ini");
    ChunkGenerator gen = new ChunkGenerator(scheme, dirName, source);
    System.out.println("create ChunkGenerator OK");

    gen.getSourceData();
    System.out.println("getSourceData OK");

    Instant startTime, endTime;
    startTime = Instant.now();
    gen.encodeChunks();
    endTime = Instant.now();
    double durationTime = Duration.between(startTime, endTime).toNanos() / 1000000.0;
    System.out.println("encodeChunks OK, " + durationTime + " ms");

    try {
      if (genNum < 0) {
        System.out.println("generate toy chunks");
        gen.generateChunks(-1);
      } else {
        for (int i = 0; i < genNum; ++i) {
          gen.generateChunks(i);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("generateChunks OK");
  }
}
