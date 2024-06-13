package io.openlineage.unshaded.spark.extension.v1;

public interface LineageExtensionProvider {

    String shadedPackage();

    default String getVisitorClassName(){
        return new StringBuffer(shadedPackage()).append(".spark.extension.v1.lifecycle.plan.SparkExtensionVisitor").toString();
    }
}
