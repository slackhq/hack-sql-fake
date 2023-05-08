namespace Slack\SQLFake;

/**
 * A simple representation of a table schema, used to make the application smarter.
 * This allows SQL Fake to provide fully typed rows, validate that columns exist,
 * enforce primary key constraints, check if indexes would be used, and more
 */
final class TableSchema implements IMemoizeParam {
	public function __construct(
		public string $name,
		public vec<Column> $fields = vec[],
		public vec<Index> $indexes = vec[],
		public ?VitessSharding $vitess_sharding = null,
	) {}

	public function getInstanceKey(): string {
		return $this->name;
	}

	public function getPrimaryKeyColumns(): keyset<string> {
		$primary = \HH\Lib\Vec\filter($this->indexes, $index ==> $index->name === 'PRIMARY')[0];
		return $primary->fields;
	}
}
