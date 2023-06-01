<?hh // strict

namespace Slack\SQLFake;

final class UpdateQuery extends Query {

	public function __construct(public from_table $updateClause, public string $sql, public bool $ignoreDupes) {}

	public vec<BinaryOperatorExpression> $setClause = vec[];

	public function execute(AsyncMysqlConnection $conn): int {
		list($tableName, $database, $data, $index_refs, $dirty_pks) = $this->processUpdateClause($conn);
		Metrics::trackQuery(QueryType::UPDATE, $conn->getServer()->name, $tableName, $this->sql);
		$schema = QueryContext::getSchema($database, $tableName);

		$columns = null;

		if ($schema is nonnull) {
			$columns = dict[];
			foreach ($schema->fields as $field) {
				$columns[$field->name] = $field;
			}
		}

		list($rows_affected, $_, $_) = $this->applyWhere(
			$conn,
			$data,
			$index_refs,
			$dirty_pks,
			$columns,
			$schema?->indexes,
		)
			|> $this->applyOrderBy($conn, $$)
			|> $this->applyLimit($$)
			|> $this->applySet(
				$conn,
				$database,
				$tableName,
				$$,
				$data,
				$index_refs,
				$dirty_pks,
				$this->setClause,
				$schema,
			);

		return $rows_affected;
	}

	/**
	 * process the UPDATE clause to retrieve the table
	 * add a row identifier to each element in the result which we can later use to update the underlying table
	 */
	protected function processUpdateClause(
		AsyncMysqlConnection $conn,
	): (string, string, dataset, index_refs, keyset<arraykey>) {
		list($database, $tableName) = Query::parseTableName($conn, $this->updateClause['name']);
		$data = $conn->getServer()->getTableData($database, $tableName) ?? tuple(dict[], dict[], keyset[]);
		return tuple($tableName, $database, $data[0], $data[1], $data[2] ?? keyset[]);
	}
}
