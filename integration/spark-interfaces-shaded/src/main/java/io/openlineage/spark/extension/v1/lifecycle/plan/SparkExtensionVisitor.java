package io.openlineage.spark.extension.v1.lifecycle.plan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.extension.v1.LineageRelationProvider;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

public final class SparkExtensionVisitor {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final OpenLineage openLineage =
      new OpenLineage(
          URI.create(
              "https://github.com/OpenLineage/OpenLineage/tree/1.0.0-SNAPSHOT/integration/spark-interfaces-shaded"));

  public boolean isDefinedAt(Object lineageNode) {
    return lineageNode instanceof LineageRelationProvider;
  }

  public Map<String, Object> apply(
      Object lineageNode, String sparkListenerEventName, Object sqlContext, Object parameters) {
    if (lineageNode instanceof LineageRelationProvider) {
      LineageRelationProvider provider = (LineageRelationProvider) lineageNode;
      DatasetIdentifier datasetIdentifier =
          provider.getLineageDatasetIdentifier(
              sparkListenerEventName, openLineage, sqlContext, parameters);
      return objectMapper.convertValue(
          datasetIdentifier, new TypeReference<Map<String, Object>>() {});
    }
    return Collections.emptyMap();
  }

  public Map<String, Object> apply(Object lineageNode) {

    return Collections.emptyMap();
  }
}
