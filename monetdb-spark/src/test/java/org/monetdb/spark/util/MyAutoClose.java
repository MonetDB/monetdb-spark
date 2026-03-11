package org.monetdb.spark.util;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Like {@link org.junit.jupiter.api.AutoClose} but without the warning if it's null.
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(MyAutoCloseExtension.class)
public @interface MyAutoClose {
}
