/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.crdt.convergent

trait ConvergentReplicatedDataType {
  def merge(that: ConvergentReplicatedDataType): ConvergentReplicatedDataType
}

