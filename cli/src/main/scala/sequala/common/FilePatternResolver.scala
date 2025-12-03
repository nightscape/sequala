package sequala.common

import java.io.File
import java.nio.file.Paths

object FilePatternResolver:

  def resolve(pattern: String): Seq[File] =
    val p = pattern.replace('\\', '/')
    val (basePath, glob) = splitPattern(p)

    val base =
      if Paths.get(basePath).isAbsolute then os.Path(basePath)
      else os.pwd / os.RelPath(basePath)

    if !os.exists(base) then Seq.empty
    else if os.isFile(base) && !glob.contains('*') && !glob.contains('?') then Seq(base.toIO)
    else if os.isDir(base) then
      val regex = globToRegex(glob)
      os.walk(base)
        .filter(p => os.isFile(p) && regex.matches(p.relativeTo(base).toString))
        .map(_.toIO)
    else Seq.empty

  private def splitPattern(pattern: String): (String, String) =
    val parts = pattern.split("/")
    val (baseParts, globParts) = parts.span(p => !p.contains('*') && !p.contains('?'))

    val basePath =
      if baseParts.isEmpty then "."
      else if pattern.startsWith("/") then "/" + baseParts.filter(_.nonEmpty).mkString("/")
      else baseParts.mkString("/")

    val glob =
      if globParts.isEmpty then ""
      else globParts.mkString("/")

    (basePath, glob)

  private def globToRegex(glob: String): scala.util.matching.Regex =
    val escaped = glob
      .replace(".", "\\.")
      .replace("**", "(.+)")
      .replace("*", "([^/]+)")
      .replace("?", "(.)")
    s"^$escaped$$".r
