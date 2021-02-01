package redis.clients.jedis.util;

import java.io.Closeable;
import java.util.NoSuchElementException;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisExhaustedPoolException;

public abstract class Pool<T> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Pool.class);

  protected GenericObjectPool<T> internalPool;

  private final int fixmeRemoveThisNumber;
  private static int fixmeNextNumber = 1;

  /**
   * Using this constructor means you have to set and initialize the internalPool yourself.
   */
  public Pool() {
    fixmeRemoveThisNumber = fixmeNextNumber++;
  }

  public Pool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
    fixmeRemoveThisNumber = fixmeNextNumber++;
    initPool(poolConfig, factory);
  }

  @Override
  public void close() {
    destroy();
  }

  public boolean isClosed() {
    return this.internalPool.isClosed();
  }

  public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {

    LOG.debug("{} Initing new pool", fixmeRemoveThisNumber);
    if (this.internalPool != null) {
      try {
        closeInternalPool();
      } catch (Exception e) {
        LOG.error("{} Could not close internal pool", fixmeRemoveThisNumber, e);
      }
    }

    this.internalPool = new GenericObjectPool<>(factory, poolConfig);
  }

  public T getResource() {
    try {
      LOG.debug("{} Borrowing objects, {}...", fixmeRemoveThisNumber, internalPool.listAllObjects());
      LOG.debug("{} Before borrowing, idle List size: {}", fixmeRemoveThisNumber, internalPool.getNumIdle());
      T borrowedObject = internalPool.borrowObject(0);
      LOG.debug("{} Borrowed object: {}", fixmeRemoveThisNumber, borrowedObject);
      LOG.debug("{} List after borrowing: {}", fixmeRemoveThisNumber, internalPool.listAllObjects());
      LOG.debug("{} After borrowing, idle List size: {}", fixmeRemoveThisNumber, internalPool.getNumIdle());
      return borrowedObject;
    } catch (NoSuchElementException nse) {
      if (null == nse.getCause()) { // The exception was caused by an exhausted pool
        throw new JedisExhaustedPoolException(
            "Could not get a resource since the pool is exhausted", nse);
      }
      // Otherwise, the exception was caused by the implemented activateObject() or ValidateObject()
      throw new JedisException("Could not get a resource from the pool", nse);
    } catch (Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool", e);
    }
  }

  protected void returnResourceObject(final T resource) {
    if (resource == null) {
      return;
    }
    try {
      LOG.debug("{} Returning object to pool: {}", fixmeRemoveThisNumber, resource);
      internalPool.returnObject(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  protected void returnBrokenResource(final T resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
    }
  }

  protected void returnResource(final T resource) {
    if (resource != null) {
      returnResourceObject(resource);
    }
  }

  public void destroy() {
    closeInternalPool();
  }

  protected void returnBrokenResourceObject(final T resource) {
    try {
      LOG.debug("{} Invalidating object: {}", fixmeRemoveThisNumber, resource);
      internalPool.invalidateObject(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the broken resource to the pool", e);
    }
  }

  protected void closeInternalPool() {
    LOG.debug("{} Closing internal pool", fixmeRemoveThisNumber);
    try {
      internalPool.close();
    } catch (Exception e) {
      throw new JedisException("Could not destroy the pool", e);
    }
  }
  
  /**
   * Returns the number of instances currently borrowed from this pool.
   *
   * @return The number of instances currently borrowed from this pool, -1 if
   * the pool is inactive.
   */
  public int getNumActive() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getNumActive();
  }
  
  /**
   * Returns the number of instances currently idle in this pool.
   *
   * @return The number of instances currently idle in this pool, -1 if the
   * pool is inactive.
   */
  public int getNumIdle() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getNumIdle();
  }
  
  /**
   * Returns an estimate of the number of threads currently blocked waiting for
   * a resource from this pool.
   *
   * @return The number of threads waiting, -1 if the pool is inactive.
   */
  public int getNumWaiters() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getNumWaiters();
  }
  
  /**
   * Returns the mean waiting time spent by threads to obtain a resource from
   * this pool.
   *
   * @return The mean waiting time, in milliseconds, -1 if the pool is
   * inactive.
   */
  public long getMeanBorrowWaitTimeMillis() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getMeanBorrowWaitTimeMillis();
  }
  
  /**
   * Returns the maximum waiting time spent by threads to obtain a resource
   * from this pool.
   *
   * @return The maximum waiting time, in milliseconds, -1 if the pool is
   * inactive.
   */
  public long getMaxBorrowWaitTimeMillis() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getMaxBorrowWaitTimeMillis();
  }

  private boolean poolInactive() {
    return this.internalPool == null || this.internalPool.isClosed();
  }

  public void addObjects(int count) {
    try {
      for (int i = 0; i < count; i++) {
        LOG.debug("{} Adding object to pool", fixmeRemoveThisNumber);
        this.internalPool.addObject();
      }
    } catch (Exception e) {
      throw new JedisException("Error trying to add idle objects", e);
    }
  }
}
