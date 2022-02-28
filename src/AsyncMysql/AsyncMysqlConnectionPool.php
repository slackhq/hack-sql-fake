<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

/**
 * Fake connection pool, instantiating fake connections instead of real ones
 * This is the main entry point to SQLFake which should be injected into your production database library,
 * replacing a regular AsyncMysqlConnectionPool
 */
final class AsyncMysqlConnectionPool extends \AsyncMysqlConnectionPool {

  private int $createdPoolConnections = 0;
  private int $destroyedPoolConnections = 0;
  private int $connectionsRequest = 0;
  private int $poolHits = 0;
  private int $poolMisses = 0;
  private static dict<string, AsyncMysqlConnection> $pool = dict[];

  public function reset(): void {
    static::$pool = dict[];
  }

  <<__Override>>
  /* HH_FIXME[4341] temp fix signature changed */
  public async function connect(
    string $host,
    int $port,
    string $dbname,
    string $_user,
    string $_password,
    int $_timeout_micros = -1,
    string $_caller = '',
    ?\MySSLContextProvider $ssl_context = null,
    int $_tcp_timeout_micros = 0,
		string $sni_server_name = "",
		string $server_cert_extensions = "",
		string $server_cert_values = "",
  ): Awaitable<AsyncMysqlConnection> {
    $this->connectionsRequest++;
    if (C\contains_key(static::$pool, $host)) {
      $this->poolHits++;
      $conn = static::$pool[$host];
      $conn->setDatabase($dbname);
      return $conn;
    }

    $this->poolMisses++;
    $this->createdPoolConnections++;
    $conn = new AsyncMysqlConnection($host, $port, $dbname);
    static::$pool[$host] = $conn;
    return $conn;
  }

  <<__Override>>
  public function connectWithOpts(
    string $host,
    int $port,
    string $dbname,
    string $user,
    string $password,
    \AsyncMysqlConnectionOptions $_conn_opts,
    string $caller = '',
  ): Awaitable<\AsyncMysqlConnection> {
    // currently, options are ignored in SQLFake
    return $this->connect($host, $port, $dbname, $user, $password, -1, $caller);
  }

  <<__Override>>
  public function getPoolStats(): darray<string, int> {
    return darray[
      'created_pool_connections' => $this->createdPoolConnections,
      'destroyed_pool_connections' => $this->destroyedPoolConnections,
      'connections_request' => $this->connectionsRequest,
      'pool_hits' => $this->poolHits,
      'pool_misses' => $this->poolMisses,
    ];
  }
}
