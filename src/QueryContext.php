<?hh // strict

namespace Slack\SQLFake;

abstract final class QueryContext {

	/**
	 * In strict mode, any query referencing a table not in the shcema
	 * will throw an exception
	 *
	 * This should be turned on if schema is available
	 */
	public static bool $strictSchemaMode = false;

	/**
	 * Emulate MySQL strict SQL mode. Invalid values for columns will
	 * throw instead of silently coercing the data
	 */
	public static bool $strictSQLMode = false;

	/**
	 * Set to true to allow unique key violations to be ignored temporarily
	 * May be useful when importing test data
	 */
	public static bool $relaxUniqueConstraints = false;

	/**
	 * Require that at least one index is used in every WHERE clause
	 */
	public static bool $requireIndexes = false;

	/**
	 * Allow violations of the must-use-index policy when the stack trace matches any of these
	 * class-function pairs.
	 */
	public static vec<shape(?'class' => string, 'function' => string)> $allowed_index_violation_traces = vec[];

	/**
	 * Require the presence of the table's Vitess sharding key
	 */
	public static bool $requireVitessShardingKey = false;

	/**
	 * Whether or not to use a replica
	 */
	public static bool $useReplica = false;

	/**
	 * Whether or not we're currently simulating a web request.
	 * This controls replica behaviour.
	 */
	public static bool $inRequest = false;

	/**
	 * 1: quiet, print nothing
	 * 2: verbose, print every query as it executes
	 * 3: very verbose, print query results as well
	 */
	public static Verbosity $verbosity = Verbosity::QUIET;

	/**
	 * Set to true to skip validating if this query will
	 * work on vitess
	 */
	public static bool $skipVitessValidation = false;

	/**
	 * If true hack-sql-fake will attempt to detect references to updated rows
	 * after they've been written in the same synthetic request.
	 */
	public static bool $preventReplicaReadsAfterWrites = false;

	public static ?string $query = null;

	/**
	 * Representation of database schema
	 * String keys are database names, with table names inside that list
	 *
	 * There is a built-in assumption that you don't have two databases on different
	 * servers with the same name but different schemas. We don't include server hostnames here
	 * because it's common to have sharded databases with the same names on different hosts
	 */
	public static dict<string, dict<string, TableSchema>> $schema = dict[];

	public static function getSchema(string $database, string $table): ?TableSchema {
		return self::$schema[$database][$table] ?? null;
	}

	public static function startRequest(): void {
		self::$inRequest = true;
	}

	public static function endRequest(): void {
		self::$inRequest = false;
		Server::cleanDirtyTables();
	}
}
