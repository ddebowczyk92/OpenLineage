/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.column.v1;

public interface FieldWithExprId extends DatasetFieldLineage {
  String getField();

  OlExprId getExprId();
}
