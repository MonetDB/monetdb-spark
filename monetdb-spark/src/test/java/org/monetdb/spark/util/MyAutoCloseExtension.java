package org.monetdb.spark.util;


import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.lang.reflect.Field;
import java.util.Optional;

public class MyAutoCloseExtension implements TestWatcher {
    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        closeAnnotatedFields(context);
        TestWatcher.super.testDisabled(context, reason);
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        closeAnnotatedFields(context);
        TestWatcher.super.testSuccessful(context);
    }

    @Override
    public void testAborted(ExtensionContext context, @Nullable Throwable cause) {
        closeAnnotatedFields(context);
        TestWatcher.super.testAborted(context, cause);
    }

    @Override
    public void testFailed(ExtensionContext context, @Nullable Throwable cause) {
        closeAnnotatedFields(context);
        TestWatcher.super.testFailed(context, cause);
    }

    private void closeAnnotatedFields(ExtensionContext context) {
        Object instance = context.getRequiredTestInstance();
        Class<?> instanceClass = instance.getClass();
        do {
            closeAnnotatedFields(instance, instanceClass);
            instanceClass = instanceClass.getSuperclass();
        } while (instanceClass != null);
    }

    private void closeAnnotatedFields(Object instance, Class<?> instanceClass) {
        for (Field field : instanceClass.getDeclaredFields()) {
            try {
                if (!field.isAnnotationPresent(MyAutoClose.class)) {
                    continue;
                }
                field.setAccessible(true);
                Object value = field.get(instance);
                if (value == null)
                    continue;

                boolean isAutoCloseable = value instanceof AutoCloseable;
                if (!isAutoCloseable)
                    continue;
                ((AutoCloseable) value).close();
                field.set(instance, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
