package fr.ps.eng.ldi.crocodile

import fr.ps.eng.ldi.crocodile.task.helper.{ANSI_RESET, AccountColorMap, PlanColorMap}

/**
 * Created by loicmdivad.
 */
trait ColorizedConsumer {

  import ColorizedConsumer._
  def colorize(line: String): String = colorizeId(line).map(colorizePlan).getOrElse(line)
}

object ColorizedConsumer {

  def colorizeId(line:String): Option[String] = "ID-\\d{3}".r.findFirstIn(line).map { id =>
    line.replaceFirst(id, s"${AccountColorMap(s"$id")}" + "$0" + s"$ANSI_RESET")
  }

  def colorizePlan(line: String): String = PlanColorMap.keys.foldLeft(line){ (line, plan) =>
    line.replace(plan, s"${PlanColorMap(plan)}$plan$ANSI_RESET")
  }
}