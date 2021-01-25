package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClusterCommand;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterMaxAttemptsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

public class JedisClusterCommandTest {

  @Test(expected = JedisClusterMaxAttemptsException.class)
  public void runZeroAttempts() {
    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(null, 0) {
      @Override
      public String execute(Jedis connection) {
        return null;
      }
    };

    testMe.run("");
  }

  @Test
  public void runSuccessfulExecute() {
    JedisClusterConnectionHandler connectionHandler = mock(JedisClusterConnectionHandler.class);
    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      @Override
      public String execute(Jedis connection) {
        return "foo";
      }
    };
    String actual = testMe.run("");
    assertEquals("foo", actual);
  }

  @Test
  public void runFailOnFirstExecSuccessOnSecondExec() {
    JedisClusterConnectionHandler connectionHandler = mock(JedisClusterConnectionHandler.class);

    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      boolean isFirstCall = true;

      @Override
      public String execute(Jedis connection) {
        if (isFirstCall) {
          isFirstCall = false;
          throw new JedisConnectionException("Borkenz");
        }

        return "foo";
      }
    };

    String actual = testMe.run("");
    assertEquals("foo", actual);
  }

  @Test
  public void runAlwaysFailing() {
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);

    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 3) {
      @Override
      public String execute(Jedis connection) {
        throw new JedisConnectionException("Connection failed");
      }
    };

    try {
      testMe.run("");
      fail("cluster command did not fail");
    } catch (JedisClusterMaxAttemptsException e) {
      // expected
    }
    InOrder inOrder = inOrder(connectionHandler);
    inOrder.verify(connectionHandler, times(3)).getConnectionFromSlot(anyInt());
    inOrder.verify(connectionHandler).renewSlotCache();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void runMovedSuccess() {
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);

    final HostAndPort movedTarget = new HostAndPort(null, 0);
    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      boolean isFirstCall = true;

      @Override
      public String execute(Jedis connection) {
        if (isFirstCall) {
          isFirstCall = false;

          // Slot 0 moved
          throw new JedisMovedDataException("", movedTarget, 0);
        }

        return "foo";
      }
    };

    String actual = testMe.run("");
    assertEquals("foo", actual);

    InOrder inOrder = inOrder(connectionHandler);
    inOrder.verify(connectionHandler).getConnectionFromSlot(anyInt());
    inOrder.verify(connectionHandler).renewSlotCache(ArgumentMatchers.<Jedis>any());
    inOrder.verify(connectionHandler).getConnectionFromNode(movedTarget);
  }

  @Test
  public void runAskSuccess() {
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);
    Jedis jedis = mock(Jedis.class);
    final HostAndPort askTarget = new HostAndPort(null, 0);
    when(connectionHandler.getConnectionFromNode(askTarget)).thenReturn(jedis);

    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      boolean isFirstCall = true;

      @Override
      public String execute(Jedis connection) {
        if (isFirstCall) {
          isFirstCall = false;

          // Slot 0 moved
          throw new JedisAskDataException("", askTarget, 0);
        }

        return "foo";
      }
    };

    String actual = testMe.run("");
    assertEquals("foo", actual);

    InOrder inOrder = inOrder(connectionHandler, jedis);
    inOrder.verify(connectionHandler).getConnectionFromSlot(anyInt());
    inOrder.verify(connectionHandler).getConnectionFromNode(askTarget);
    inOrder.verify(jedis).asking();
  }

  @Test
  public void runMovedAndFailing() {
    // Test:
    // First attempt is a JedisMovedDataException() move, because we asked the wrong node
    // Second attempt is a JedisConnectionException, because this node is down
    // In response to that, runWithTimeout() retires the same node again forever, see verifications
    // at the bottom of the test
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);

    final Jedis fromGetConnectionFromSlot = mock(Jedis.class);
    when(connectionHandler.getConnectionFromSlot(anyInt())).thenReturn(fromGetConnectionFromSlot);

    final Jedis fromGetConnectionFromNode = mock(Jedis.class);
    when(connectionHandler.getConnectionFromNode(any(HostAndPort.class))).thenReturn(
      fromGetConnectionFromNode);

    final HostAndPort movedTarget = new HostAndPort(null, 0);
    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 3) {
      @Override
      public String execute(Jedis connection) {
        if (fromGetConnectionFromSlot == connection) {
          // First attempt, report moved
          throw new JedisMovedDataException("Moved", movedTarget, 0);
        }

        if (fromGetConnectionFromNode == connection) {
          // Second attempt in response to the move, report failure
          throw new JedisConnectionException("Connection failed");
        }

        throw new IllegalStateException("Should have thrown jedis exception");
      }
    };

    try {
      testMe.run("");
      fail("cluster command did not fail");
    } catch (JedisClusterMaxAttemptsException e) {
      // expected
    }
    InOrder inOrder = inOrder(connectionHandler);
    inOrder.verify(connectionHandler).getConnectionFromSlot(anyInt());
    inOrder.verify(connectionHandler).renewSlotCache(fromGetConnectionFromSlot);
    inOrder.verify(connectionHandler, times(2)).getConnectionFromNode(movedTarget);
    inOrder.verify(connectionHandler).renewSlotCache();
    inOrder.verifyNoMoreInteractions();
  }

  @Test(expected = JedisNoReachableClusterNodeException.class)
  public void runRethrowsJedisNoReachableClusterNodeException() {
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);
    when(connectionHandler.getConnectionFromSlot(anyInt())).thenThrow(
      JedisNoReachableClusterNodeException.class);

    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      @Override
      public String execute(Jedis connection) {
        return null;
      }
    };

    testMe.run("");
  }
}
