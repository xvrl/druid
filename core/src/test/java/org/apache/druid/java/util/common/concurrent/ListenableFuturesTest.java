/*
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

package org.apache.druid.java.util.common.concurrent;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import junit.framework.AssertionFailedError;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static org.apache.druid.java.util.common.concurrent.Execs.directExecutor;
import static org.junit.Assert.*;

public class ListenableFuturesTest
{
  public static <V> V getDone(Future<V> future) throws ExecutionException {
    /*
     * We throw IllegalStateException, since the call could succeed later. Perhaps we "should" throw
     * IllegalArgumentException, since the call could succeed with a different argument. Those
     * exceptions' docs suggest that either is acceptable. Google's Java Practices page recommends
     * IllegalArgumentException here, in part to keep its recommendation simple: Static methods
     * should throw IllegalStateException only when they use static state.
     *
     *
     * Why do we deviate here? The answer: We want for fluentFuture.getDone() to throw the same
     * exception as Futures.getDone(fluentFuture).
     */
    checkState(future.isDone(), "Future was expected to be done: %s", future);
    return getUninterruptibly(future);
  }

  static final class UncheckedThrowingFuture<V> extends AbstractFuture<V>
  {

    public static <V> ListenableFuture<V> throwingError(Error error) {
      UncheckedThrowingFuture<V> future = new UncheckedThrowingFuture<V>();
      future.complete(checkNotNull(error));
      return future;
    }

    public static <V> ListenableFuture<V> throwingRuntimeException(RuntimeException e) {
      UncheckedThrowingFuture<V> future = new UncheckedThrowingFuture<V>();
      future.complete(checkNotNull(e));
      return future;
    }

    public static <V> UncheckedThrowingFuture<V> incomplete() {
      return new UncheckedThrowingFuture<V>();
    }

    public void complete(RuntimeException e) {
      if (!super.setException(new WrapperException(checkNotNull(e)))) {
        throw new IllegalStateException("Future was already complete: " + this);
      }
    }

    public void complete(Error e) {
      if (!super.setException(new WrapperException(checkNotNull(e)))) {
        throw new IllegalStateException("Future was already complete: " + this);
      }
    }

    private static final class WrapperException extends Exception {
      WrapperException(Throwable t) {
        super(t);
      }
    }

    private static void rethrow(ExecutionException e) throws ExecutionException {
      Throwable wrapper = e.getCause();
      if (wrapper instanceof WrapperException) {
        Throwable cause = wrapper.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        }
      }
      throw e;
    }

    @Override
    public V get() throws ExecutionException, InterruptedException {
      try {
        super.get();
      } catch (ExecutionException e) {
        rethrow(e);
      }
      throw new AssertionError("Unreachable");
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
      try {
        super.get(timeout, unit);
      } catch (ExecutionException e) {
        rethrow(e);
      }
      throw new AssertionError("Unreachable");
    }
  }

  // Class hierarchy for generics sanity checks
  private static class Foo {}

  private static class Bar {}

  static class MyError extends Error {}

  static class MyRuntimeException extends RuntimeException {}

  private static <V> Function<V, ListenableFuture<V>> asyncIdentity()
  {
    return Futures::immediateFuture;
  }

  @Test
  public void testTransformAsync_cancelPropagatesToInput() {
    SettableFuture<Foo> input = SettableFuture.create();
    Function<Foo, ListenableFuture<Bar>> function = (Foo unused) -> {
            throw new AssertionFailedError("Unexpeted call to apply.");
          };
    assertTrue(ListenableFutures.transformAsync(input, function).cancel(false));
    assertTrue(input.isCancelled());
    //assertFalse(input.wasInterrupted());
  }

  @Test
  public void testTransformAsync_interruptPropagatesToInput() {
    SettableFuture<Foo> input = SettableFuture.create();
    Function<Foo, ListenableFuture<Bar>> function = (Foo unused) -> {
            throw new AssertionFailedError("Unexpeted call to apply.");
          };
    assertTrue(ListenableFutures.transformAsync(input, function).cancel(true));
    assertTrue(input.isCancelled());
    //assertTrue(input.wasInterrupted());
  }

  @Test
  public void testTransformAsync_cancelPropagatesToAsyncOutput() {
    ListenableFuture<Foo> immediate = Futures.immediateFuture(new Foo());
    final SettableFuture<Bar> secondary = SettableFuture.create();
    Function<Foo, ListenableFuture<Bar>> function  = (Foo unused) -> {
            return secondary;
          };
    assertTrue(ListenableFutures.transformAsync(immediate, function).cancel(false));
    assertTrue(secondary.isCancelled());
    //assertFalse(secondary.wasInterrupted());
  }

  @Test
  public void testTransformAsync_interruptPropagatesToAsyncOutput() {
    ListenableFuture<Foo> immediate = Futures.immediateFuture(new Foo());
    final SettableFuture<Bar> secondary = SettableFuture.create();
    Function<Foo, ListenableFuture<Bar>> function = (Foo unused) -> {
            return secondary;
          };
    assertTrue(ListenableFutures.transformAsync(immediate, function).cancel(true));
    assertTrue(secondary.isCancelled());
    //assertTrue(secondary.wasInterrupted());
  }

  @Test
  public void testTransformAsync_inputCancelButNotInterruptPropagatesToOutput() {
    SettableFuture<Foo> f1 = SettableFuture.create();
    final SettableFuture<Bar> secondary = SettableFuture.create();
    Function<Foo, ListenableFuture<Bar>> function = (Foo unused) -> {
            return secondary;
          };
    ListenableFuture<Bar> f2 = ListenableFutures.transformAsync(f1, function);
    f1.cancel(true);
    assertTrue(f2.isCancelled());
    /*
     * We might like to propagate interruption, too, but it's not clear that it matters. For now, we
     * test for the behavior that we have today.
     */
    //assertFalse(((AbstractFuture<?>) f2).wasInterrupted());
  }

  @Test
  public void testTransformAsync_ErrorAfterCancellation() {
    class Transformer implements Function<Object, ListenableFuture<Object>> {
      ListenableFuture<Object> output;

      @Override
      public ListenableFuture<Object> apply(Object input) {
        output.cancel(false);
        throw new MyError();
      }
    }
    Transformer transformer = new Transformer();
    SettableFuture<Object> input = SettableFuture.create();

    ListenableFuture<Object> output = ListenableFutures.transformAsync(input, transformer);
    transformer.output = output;

    input.set("foo");
    assertTrue(output.isCancelled());
  }

  @Test
  public void testTransformAsync_ExceptionAfterCancellation(){
    class Transformer implements Function<Object, ListenableFuture<Object>> {
      ListenableFuture<Object> output;

      @Override
      public ListenableFuture<Object> apply(Object input) {
        output.cancel(false);
        throw new MyRuntimeException();
      }
    }
    Transformer transformer = new Transformer();
    SettableFuture<Object> input = SettableFuture.create();

    ListenableFuture<Object> output = ListenableFutures.transformAsync(input, transformer);
    transformer.output = output;

    input.set("foo");
    assertTrue(output.isCancelled());
  }

  @Test
  public void testTransformAsync_getThrowsRuntimeException() {
    ListenableFuture<Object> input =
        UncheckedThrowingFuture.throwingRuntimeException(new MyRuntimeException());

    ListenableFuture<Object> output = ListenableFutures.transformAsync(input, asyncIdentity());
    try {
      getDone(output);
      fail();
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() instanceof MyRuntimeException);
    }
  }

  @Test
  public void testTransformAsync_getThrowsError() {
    ListenableFuture<Object> input = UncheckedThrowingFuture.throwingError(new MyError());

    ListenableFuture<Object> output = ListenableFutures.transformAsync(input, asyncIdentity());
    try {
      getDone(output);
      fail();
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() instanceof MyError);
    }
  }

  @Test
  public void testTransformAsync_listenerThrowsError() {
    SettableFuture<Object> input = SettableFuture.create();
    ListenableFuture<Object> output = ListenableFutures.transformAsync(input, asyncIdentity());

    output.addListener(
        new Runnable() {
          @Override
          public void run() {
            throw new MyError();
          }
        },
        directExecutor());
    try {
      input.set("foo");
      fail();
    } catch (MyError expected) {
    }
  }

  @Test
  public void testTransformAsync_asyncFunction_error() {
    final Error error = new Error("deliberate");
    Function<String, ListenableFuture<Integer>> function = (String input) -> {
            throw error;
          };
    SettableFuture<String> inputFuture = SettableFuture.create();
    ListenableFuture<Integer> outputFuture =
        ListenableFutures.transformAsync(inputFuture, function);
    inputFuture.set("value");
    try {
      getDone(outputFuture);
      fail();
    } catch (ExecutionException expected) {
      assertSame(error, expected.getCause());
    }
  }
}
