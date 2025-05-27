package dev.vortex.iceberg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

final class ConversionUtil {

  /** Convert a Parquet file to a Vortex file using the {@code vx} CLI tool. */
  static Path parquetToVortex(Path input) {
    try {
      Process convert =
          new ProcessBuilder()
              .command("vx", "convert", input.toAbsolutePath().toString())
              .redirectError(ProcessBuilder.Redirect.PIPE)
              .redirectOutput(ProcessBuilder.Redirect.PIPE)
              .start();
      int exitValue = convert.waitFor();
      if (convert.exitValue() != 0) {
        String error = "vx convert failed (code = " + convert.exitValue() + ")";
        error += "\n";
        error +=
            "vx convert stdout: "
                + new String(convert.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        error += "\n";
        error +=
            "vx convert stderr: "
                + new String(convert.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        throw new RuntimeException(error);
      }

      // Return the output path
      String rewrittenName = input.getFileName().toString().replace(".parquet", ".vortex");
      return input.resolveSibling(rewrittenName);

    } catch (IOException e) {
      throw new RuntimeException("Conversion error", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Process interrupted while waiting child", e);
    }
  }
}
