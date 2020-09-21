object StartApp extends App {

  val app = "idg1demo"

  println(s"HELLO from $app")

  if (args.length != 3) {
    println("Expect 3 parameters:")
    println(
      (Stream.from(1) zip
        Seq(
          "input file (CSV)",
          "rename instructions (JSON)",
          "output file (JSON)"
        )).map { case (n, s) => s"\t$n) $s" }.mkString("\n"))
    println
  } else {
    MainLogic.apply(args(0), args(1), args(2))
  }

  println(s"BYE from $app")
}
