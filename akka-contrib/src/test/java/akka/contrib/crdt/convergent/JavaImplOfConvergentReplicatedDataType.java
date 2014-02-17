/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.crdt.convergent;

public class JavaImplOfConvergentReplicatedDataType extends
    ConvergentReplicatedDataTypeBase {

  @Override
  public JavaImplOfConvergentReplicatedDataType merge(
      ConvergentReplicatedDataType other) {
    return this;
  }
}
