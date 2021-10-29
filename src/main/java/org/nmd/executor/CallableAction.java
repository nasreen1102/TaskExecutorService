package org.nmd.executor;

public interface CallableAction<I, O> {
    O action(I input);
}
