package com.github.zk1931.zabkv;

import java.io.Serializable;

/**
 * Base class for all the commands.
 */
public abstract class Command implements Serializable {

    private static final long serialVersionUID = 0L;

    abstract void execute(Database sb);
}
