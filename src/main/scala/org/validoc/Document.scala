package org.validoc

trait Document

case class ProductionId(raw: String) extends AnyVal

case class ThorData(raw: String) extends Document

case class MercuryData(raw: String) extends Document

