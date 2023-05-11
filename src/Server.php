<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict};

final class Server {

	public function __construct(public string $name, public ?server_config $config = null) {}

	private static dict<string, Server> $instances = dict[];
	private static keyset<string> $snapshot_names = keyset[];
	/**
	 * The main storage mechanism
	 * dict of strings (database schema names)
	 * -> dict of string table names to tables
	 * -> vec of rows
	 * -> dict of string column names to columns
	 *
	 * While a structure based on objects all the way down the stack may be more powerful and readable,
	 * This structure uses value types intentionally, to enable a relatively efficient reset/snapshot logic
	 * which is often used frequently between test cases
	 */
	public dict<string, database> $databases = dict[];
	private dict<string, dict<string, database>> $snapshots = dict[];

	public static function getAll(): dict<string, this> {
		return static::$instances;
	}

	public static function getAllTables(): dict<string, dict<string, database>> {
		return Dict\map(static::getAll(), ($server) ==> {
			return $server->databases;
		});
	}

	/**
	 * This will override everything in $instances
	 */
	public static function setAll(dict<string, Server> $instances): void {
		static::$instances = $instances;
	}

	public static function get(string $name): ?this {
		return static::$instances[$name] ?? null;
	}

	public function setConfig(server_config $config): void {
		$this->config = $config;
	}

	public static function getOrCreate(string $name): this {
		$server = static::$instances[$name] ?? null;
		if ($server === null) {
			$server = new static($name);
			static::$instances[$name] = $server;
		}
		return $server;
	}

	public static function cloneByName(string $name, string $clone_name): this {
		$clone = static::get($clone_name);
		if ($clone === null) {
			throw new SQLFakeRuntimeException("Server $clone_name not found, unable to clone databases and snapshots");
		}

		$server = static::getOrCreate($name);
		$server->databases = $clone->databases;
		$server->snapshots = $clone->snapshots;
		return $server;
	}

	public static function reset(): void {
		foreach (static::getAll() as $server) {
			$server->doReset();
		}
	}

	public static function snapshot(string $name): void {
		foreach (static::getAll() as $server) {
			$server->doSnapshot($name);
		}
		static::$snapshot_names[] = $name;
	}

	public static function restore(string $name): void {
		if (!C\contains_key(static::$snapshot_names, $name)) {
			throw new SQLFakeRuntimeException("Snapshot $name not found, unable to restore");
		}
		foreach (static::getAll() as $server) {
			$server->doRestore($name);
		}
	}

	protected function doSnapshot(string $name): void {
		$this->snapshots[$name] = $this->databases;
	}

	protected function doRestore(string $name): void {
		$this->databases = $this->snapshots[$name] ?? dict[];
	}

	protected function doReset(): void {
		$this->databases = dict[];
	}

	/**
	 * Retrieve a table from the specified database, if it exists, by value
	 */
	public function getTableData(string $dbname, string $name): ?table_data {
		return $this->databases[$dbname][$name] ?? null;
	}

	/**
	 * Save a table's rows back to the database
	 * note, because insert and update operations already grab the full table for checking constraints,
	 * we don't bother providing an insert or update helper here.
	 */
	public function saveTable(
		string $dbname,
		string $name,
		dataset $rows,
		unique_index_refs $unique_index_refs,
		index_refs $index_refs,
	): void {
		// create table if not exists
		if (!C\contains_key($this->databases, $dbname)) {
			$this->databases[$dbname] = dict[];
		}

		// save rows
		$this->databases[$dbname][$name] = tuple($rows, $unique_index_refs, $index_refs);
	}
}
