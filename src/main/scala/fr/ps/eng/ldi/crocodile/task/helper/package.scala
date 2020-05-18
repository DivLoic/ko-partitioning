package fr.ps.eng.ldi.crocodile.task

/**
 * Created by loicmdivad.
 */
package object helper {
  val ANSI_RESET = "\u001B[0m"
  val ANSI_BLACK = "\u001B[1;30m"
  val ANSI_RED = "\u001B[1;31m"
  val ANSI_GREEN = "\u001B[1;32m"
  val ANSI_YELLOW = "\u001B[1;33m"
  val ANSI_BLUE = "\u001B[1;34m"
  val ANSI_PURPLE = "\u001B[1;35m"
  val ANSI_CYAN = "\u001B[1;36m"
  val ANSI_WHITE = "\u001B[1;37m"
  val ANSI_GREY = "\u001B[1;90m"

  val ANSI_BLACK_BACKGROUND = "\u001B[40m"
  val ANSI_RED_BACKGROUND = "\u001B[41m"
  val ANSI_GREEN_BACKGROUND = "\u001B[42m"
  val ANSI_YELLOW_BACKGROUND = "\u001B[43m"
  val ANSI_BLUE_BACKGROUND = "\u001B[44m"
  val ANSI_PURPLE_BACKGROUND = "\u001B[45m"
  val ANSI_CYAN_BACKGROUND = "\u001B[46m"
  val ANSI_WHITE_BACKGROUND = "\u001B[47m"
  val ANSI_GREY_BACKGROUND = "\u001B[100m"

  val AccountColorMap = Map(
    "ID-000" -> ANSI_RED,
    "ID-001" -> ANSI_GREEN,
    "ID-002" -> ANSI_CYAN,
    "ID-003" -> ANSI_PURPLE
  )

  val PlanColorMap = Map(
    "Gold" -> ANSI_YELLOW,
    "Plus" -> ANSI_GREY
  )
}
