<?hh // strict

namespace Slack\SQLFake;

final class UpdateQuery extends Query {

	public function __construct(public from_table $updateClause, public string $sql, public bool $ignoreDupes) {}

	public vec<BinaryOperatorExpression> $setClause = vec[];

	public function execute(AsyncMysqlConnection $conn): int {
		list($tableName, $database, $data) = $this->processUpdateClause($conn);
		Metrics::trackQuery(QueryType::UPDATE, $conn->getServer()->name, $tableName, $this->sql);
		$schema = QueryContext::getSchema($database, $tableName);

		list($rows_affected, $_) = $this->applyWhere($conn, $data)
			|> $this->applyOrderBy($conn, $$)
			|> $this->applyLimit($$)
			|> $this->applySet($conn, $database, $tableName, $$, $data, $this->setClause, $schema);

		return $rows_affected;
	}

	/**
	 * process the UPDATE clause to retrieve the table
	 * add a row identifier to each element in the result which we can later use to update the underlying table
	 */
	protected function processUpdateClause(AsyncMysqlConnection $conn): (string, string, dataset) {
		list($database, $tableName) = Query::parseTableName($conn, $this->updateClause['name']);
		$table = $conn->getServer()->getTable($database, $tableName) ?? vec[];
		return tuple($tableName, $database, $table);
	}
}
