package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.unshaded.spark.extension.v1.LineageExtensionProvider;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

@Slf4j
public final class SparkExtensionVisitorWrapper {

  private final List<Object> extensionObjects;
  private final boolean hasLoadedObjects;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(
          new SimpleModule().addDeserializer(DatasetIdentifier.class, new DatasetIdentifierDeserializer()));

  private SparkExtensionVisitorWrapper() {
    try {
      extensionObjects = init();
      this.hasLoadedObjects = !extensionObjects.isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isDefinedAt(Object object) {
    return hasLoadedObjects
        && extensionObjects.stream()
            .map(o -> getMethod(o, "isDefinedAt", Object.class))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .anyMatch(
                objectAndMethod -> {
                  try {
                    return (boolean) objectAndMethod.right.invoke(objectAndMethod.left, object);
                  } catch (Exception e) {
                    log.error(
                        "Can't invoke 'isDefinedAt' method on {} class instance",
                        objectAndMethod.left.getClass().getCanonicalName());
                  }
                  return false;
                });
  }

  public Map<String, Object> apply(Object object) {
    if (!hasLoadedObjects) {
      return Collections.emptyMap();
    } else {
      final List<ImmutablePair<Object, Method>> methodsToCall =
          extensionObjects.stream()
              .map(o -> getMethod(o, "apply", Object.class))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());

      for (ImmutablePair<Object, Method> objectAndMethod : methodsToCall) {
        try {
          Map<String, Object> result =
              (Map<String, Object>) objectAndMethod.right.invoke(objectAndMethod.left, object);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          log.error(
              "Can't invoke apply method on {} class instance",
              objectAndMethod.left.getClass().getCanonicalName());
        }
      }
    }
    return Collections.emptyMap();
  }

  public DatasetIdentifier apply(
      Object lineageNode, String sparkListenerEventName, Object sqlContext, Object parameters) {
    if (!hasLoadedObjects) {
      return null;
    } else {
      final List<ImmutablePair<Object, Method>> methodsToCall =
          extensionObjects.stream()
              .map(
                  o ->
                      getMethod(o, "apply", Object.class, String.class, Object.class, Object.class))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());

      for (ImmutablePair<Object, Method> objectAndMethod : methodsToCall) {
        try {
          Map<String, Object> result =
              (Map<String, Object>)
                  objectAndMethod.right.invoke(
                      objectAndMethod.left,
                      lineageNode,
                      sparkListenerEventName,
                      sqlContext,
                      parameters);
          if (result != null) {
            return objectMapper.convertValue(result, DatasetIdentifier.class);
          }
        } catch (Exception e) {
          log.warn(
              "Can't invoke apply method on {} class instance",
              objectAndMethod.left.getClass().getCanonicalName());
        }
      }
    }
    return null;
  }

  private Optional<ImmutablePair<Object, Method>> getMethod(
      Object classInstance, String methodName, Class<?>... parameterTypes) {
    try {
      Method method = classInstance.getClass().getMethod(methodName, parameterTypes);
      method.setAccessible(true);
      return Optional.of(ImmutablePair.of(classInstance, method));
    } catch (NoSuchMethodException e) {
      log.warn(
          "No '{}' method found on {} class instance",
          methodName,
          classInstance.getClass().getCanonicalName());
    }
    return Optional.empty();
  }

  public static SparkExtensionVisitorWrapper getInstance() {
    return InstanceHolder.instance;
  }

  private static List<Object> init()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    List<Object> objects = new ArrayList<>();
    ServiceLoader<LineageExtensionProvider> serviceLoader =
        ServiceLoader.load(LineageExtensionProvider.class);
    for (LineageExtensionProvider service : serviceLoader) {
      String className = service.getVisitorClassName();
      Class<?> loadedClass = Class.forName(className);
      Object classInstance = loadedClass.newInstance();
      objects.add(classInstance);
    }
    return objects;
  }

  private static class InstanceHolder {
    private static final SparkExtensionVisitorWrapper instance = new SparkExtensionVisitorWrapper();
  }

  private static class DatasetIdentifierDeserializer extends JsonDeserializer<DatasetIdentifier> {
    @Override
    public DatasetIdentifier deserialize(JsonParser jsonParser, DeserializationContext ctx) throws IOException, JsonProcessingException {
      JsonNode node = jsonParser.getCodec().readTree(jsonParser);
      final String name = node.get("name").asText();
      final String namespace =  node.get("namespace").asText();
      DatasetIdentifier identifier = new DatasetIdentifier(name, namespace);
      ArrayNode symlinks = (ArrayNode) node.get("symlinks");
      final Iterator<JsonNode> symlinksIter = symlinks.elements();

      while(symlinksIter.hasNext()){
        JsonNode symlinkNode = symlinksIter.next();
        DatasetIdentifier.Symlink symlink = ctx.readValue(symlinkNode.traverse(), DatasetIdentifier.Symlink.class);
        identifier.withSymlink(symlink);
      }
      return identifier;
    }
  }
}
