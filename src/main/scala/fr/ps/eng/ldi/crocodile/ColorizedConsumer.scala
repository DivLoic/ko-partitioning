package fr.ps.eng.ldi.crocodile

import fr.ps.eng.ldi.crocodile.task.helper.{ANSI_RESET, AccountColorMap}

/**
 * Created by loicmdivad.
 */
trait ColorizedConsumer {

  def colorize(line: String): String = "ID-\\d{3}".r.findFirstIn(line).map { id =>

    line.replaceFirst(id, s"${AccountColorMap(s"$id")}" + "$0" + s"$ANSI_RESET")

  }.getOrElse(line)

}
