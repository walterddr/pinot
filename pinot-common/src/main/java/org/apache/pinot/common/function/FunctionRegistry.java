/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.util.NameMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for scalar functions.
 * <p>TODO: Merge FunctionRegistry and FunctionDefinitionRegistry to provide one single registry for all functions.
 */
public class FunctionRegistry {
  private FunctionRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);

  // TODO: consolidate the following 2
  // This FUNCTION_INFO_MAP is used by Pinot server to look up function by # of arguments
  private static final Map<String, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP = new HashMap<>();
  // This FUNCTION_MAP is used by Calcite function catalog to look up function by function signature.
  private static final NameMultimap<Function> FUNCTION_MAP = new NameMultimap<>();

  private static final int VAR_ARG_KEY = -1;

  /**
   * Registers the scalar functions via reflection.
   * NOTE: In order to plugin methods using reflection, the methods should be inside a class that includes ".function."
   *       in its class path. This convention can significantly reduce the time of class scanning.
   */
  static {
    long startTimeMs = System.currentTimeMillis();
    Set<Method> methods = PinotReflectionUtils.getMethodsThroughReflection(".*\\.function\\..*", ScalarFunction.class);
    for (Method method : methods) {
      if (!Modifier.isPublic(method.getModifiers())) {
        continue;
      }
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (scalarFunction.enabled()) {
        // Annotated function names
        String[] scalarFunctionNames = scalarFunction.names();
        boolean nullableParameters = scalarFunction.nullableParameters();
        boolean isPlaceholder = scalarFunction.isPlaceholder();
        boolean isVarArg = scalarFunction.isVarArg();
        if (scalarFunctionNames.length > 0) {
          for (String name : scalarFunctionNames) {
            FunctionRegistry.registerFunction(name, method, nullableParameters, isPlaceholder, isVarArg);
          }
        } else {
          FunctionRegistry.registerFunction(method, nullableParameters, isPlaceholder, isVarArg);
        }
      }
    }
    LOGGER.info("Initialized FunctionRegistry with {} functions: {} in {}ms", FUNCTION_INFO_MAP.size(),
        FUNCTION_INFO_MAP.keySet(), System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  /**
   * Registers a method with the name of the method.
   */
  public static void registerFunction(Method method, boolean nullableParameters, boolean isPlaceholder,
      boolean isVarArg) {
    registerFunction(method.getName(), method, nullableParameters, isPlaceholder, isVarArg);
  }

  /**
   * Registers a method with the given function name.
   */
  public static void registerFunction(String functionName, Method method, boolean nullableParameters,
      boolean isPlaceholder, boolean isVarArg) {
    if (!isPlaceholder) {
      registerFunctionInfoMap(functionName, method, nullableParameters, isVarArg);
    }
    registerCalciteNamedFunctionMap(functionName, method, nullableParameters, isVarArg);
  }

  private static void registerFunctionInfoMap(String functionName, Method method, boolean nullableParameters,
      boolean isVarArg) {
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass(), nullableParameters);
    String canonicalName = canonicalize(functionName);
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.computeIfAbsent(canonicalName, k -> new HashMap<>());
    if (isVarArg) {
      FunctionInfo existFunctionInfo = functionInfoMap.put(VAR_ARG_KEY, functionInfo);
      Preconditions.checkState(existFunctionInfo == null || existFunctionInfo.getMethod() == functionInfo.getMethod(),
          "Function: %s with variable number of parameters is already registered", functionName);
    } else {
      FunctionInfo existFunctionInfo = functionInfoMap.put(method.getParameterCount(), functionInfo);
      Preconditions.checkState(existFunctionInfo == null || existFunctionInfo.getMethod() == functionInfo.getMethod(),
          "Function: %s with %s parameters is already registered", functionName, method.getParameterCount());
    }
  }

  private static void registerCalciteNamedFunctionMap(String functionName, Method method, boolean nullableParameters,
      boolean isVarArg) {
    if (method.getAnnotation(Deprecated.class) == null) {
      FUNCTION_MAP.put(functionName, ScalarFunctionImpl.create(method));
    }
  }

  public static Map<String, List<Function>> getRegisteredCalciteFunctionMap() {
    return FUNCTION_MAP.map();
  }

  public static Set<String> getRegisteredCalciteFunctionNames() {
    return FUNCTION_MAP.map().keySet();
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    return FUNCTION_INFO_MAP.containsKey(canonicalize(functionName));
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParameters) {
    Map<Integer, FunctionInfo> functionInfoMap = FUNCTION_INFO_MAP.get(canonicalize(functionName));
    if (functionInfoMap != null) {
      FunctionInfo functionInfo = functionInfoMap.get(numParameters);
      if (functionInfo != null) {
        return functionInfo;
      }
      return functionInfoMap.get(VAR_ARG_KEY);
    }
    return null;
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  /**
   * Placeholders for scalar function, they register and represents the signature for transform and filter predicate
   * so that v2 engine can understand and plan them correctly.
   */
  private static class PlaceholderScalarFunctions {

    @ScalarFunction(names = {"textContains", "text_contains"}, isPlaceholder = true)
    public static boolean textContains(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"textMatch", "text_match"}, isPlaceholder = true)
    public static boolean textMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonMatch", "json_match"}, isPlaceholder = true)
    public static boolean jsonMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"vectorSimilarity", "vector_similarity"}, isPlaceholder = true)
    public static double vectorSimilarity(float[] vector1, float[] vector2) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }
  }
}
