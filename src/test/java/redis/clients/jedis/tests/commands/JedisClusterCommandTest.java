package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
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
  public void runReconnectWithRandomConnection() {
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);
    // simulate failing connection
    when(connectionHandler.getConnectionFromSlot(anyInt())).thenReturn(null);
    // simulate good connection
    when(connectionHandler.getConnection()).thenReturn(mock(Jedis.class));

    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      @Override
      public String execute(Jedis connection) {
        if (connection == null) {
          throw new JedisConnectionException("");
        }
        return "foo";
      }
    };

    String actual = testMe.run("");
    assertEquals("foo", actual);
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
  public void runMovedFailSuccess() {
    // Test:
    // First attempt is a JedisMovedDataException() move, because we asked the wrong node
    // Second attempt is a JedisConnectionException, because this node is down
    // In response to that, runWithTimeout() requests a random node using
    // connectionHandler.getConnection()
    // Third attempt works
    JedisSlotBasedConnectionHandler connectionHandler = mock(JedisSlotBasedConnectionHandler.class);

    Jedis fromGetConnectionFromSlot = mock(Jedis.class);
    when(fromGetConnectionFromSlot.toString()).thenReturn("getConnectionFromSlot");
    when(connectionHandler.getConnectionFromSlot(anyInt())).thenReturn(fromGetConnectionFromSlot);

    Jedis fromGetConnectionFromNode = mock(Jedis.class);
    when(fromGetConnectionFromNode.toString()).thenReturn("getConnectionFromNode");
    when(connectionHandler.getConnectionFromNode(any(HostAndPort.class))).thenReturn(
      fromGetConnectionFromNode);

    Jedis fromGetConnection = mock(Jedis.class);
    when(fromGetConnection.toString()).thenReturn("getConnection");
    when(connectionHandler.getConnection()).thenReturn(fromGetConnection);

    final HostAndPort movedTarget = new HostAndPort(null, 0);
    JedisClusterCommand<String> testMe = new JedisClusterCommand<String>(connectionHandler, 10) {
      @Override
      public String execute(Jedis connection) {
        String source = connection.toString();
        if ("getConnectionFromSlot".equals(source)) {
          // First attempt, report moved
          throw new JedisMovedDataException("Moved", movedTarget, 0);
        }

        if ("getConnectionFromNode".equals(source)) {
          // Second attempt in response to the move, report failure
          throw new JedisConnectionException("Connection failed");
        }

        // This is the third and last case we handle
        assert "getConnection".equals(source);
        return "foo";
      }
    };

    String actual = testMe.run("");
    assertEquals("foo", actual);
    InOrder inOrder = inOrder(connectionHandler);
    inOrder.verify(connectionHandler).getConnectionFromSlot(anyInt());
    inOrder.verify(connectionHandler).renewSlotCache(fromGetConnectionFromSlot);
    inOrder.verify(connectionHandler).getConnectionFromNode(movedTarget);
    inOrder.verify(connectionHandler).getConnection();
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
