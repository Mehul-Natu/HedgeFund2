import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object FileDistributor extends App {
  val sourceDir = new File("src/main/fetched_data")
  val destDirBasePath = "src/main/distributed_data"
  val maxFilesPerFolder = 40
  val totalFiles = 494

  if (!sourceDir.exists() || !sourceDir.isDirectory) {
    println("Source directory does not exist or is not a directory.")
  } else {
    val files = sourceDir.listFiles().filter(_.isFile)

    if (files.length != totalFiles) {
      println(s"Expected $totalFiles files, but found ${files.length} files.")
    } else {
      // Calculate the number of folders needed
      val numFolders = (totalFiles + maxFilesPerFolder - 1) / maxFilesPerFolder

      for (i <- 0 until numFolders) {
        val destDir = new File(s"$destDirBasePath/folder${i + 1}")
        if (!destDir.exists()) destDir.mkdirs()

        // Calculate start and end indices for file slicing
        val startIndex = i * maxFilesPerFolder
        val endIndex = Math.min(startIndex + maxFilesPerFolder, totalFiles)

        files.slice(startIndex, endIndex).foreach { file =>
          try {
            Files.move(
              Paths.get(file.getAbsolutePath),
              Paths.get(destDir.getAbsolutePath, file.getName),
              StandardCopyOption.REPLACE_EXISTING
            )
          } catch {
            case e: Exception => println(s"Failed to move file: ${file.getName}. Error: ${e.getMessage}")
          }
        }
      }
      println("Files have been distributed.")
    }
  }
}
