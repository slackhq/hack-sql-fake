<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Keyset};

final class DeleteQuery extends Query {
	public ?from_table $fromClause = null;

	public function __construct(public string $sql) {}

	public function execute(AsyncMysqlConnection $conn): int {
		$this->fromClause as nonnull;
		list($database, $table_name) = Query::parseTableName($conn, $this->fromClause['name']);
		$data = $conn->getServer()->getTableData($database, $table_name) ?? tuple(dict[], dict[]);
		$schema = QueryContext::getSchema($database, $table_name);

		Metrics::trackQuery(QueryType::DELETE, $conn->getServer()->name, $table_name, $this->sql);

		$columns = null;

		if ($schema is nonnull) {
			$columns = dict[];
			foreach ($schema->fields as $field) {
				$columns[$field->name] = $field;
			}
		}

		return $this->applyWhere($conn, $data[0], $data[1], $columns, $schema?->indexes)
			|> $this->applyOrderBy($conn, $$)
			|> $this->applyLimit($$)
			|> $this->applyDelete($conn, $database, $table_name, $$, $data[0], $data[1], $schema);
	}

	/**
	 * Delete rows after all filtering clauses, and return the number of rows deleted
	 */
	protected function applyDelete(
		AsyncMysqlConnection $conn,
		string $database,
		string $table_name,
		dataset $filtered_rows,
		dataset $original_table,
		index_refs $index_refs,
		?TableSchema $table_schema,
	): int {
		$rows_to_delete = Keyset\keys($filtered_rows);
		$remaining_rows =
			Dict\filter_with_key($original_table, ($row_num, $_) ==> !C\contains_key($rows_to_delete, $row_num));
		$rows_affected = C\count($original_table) - C\count($remaining_rows);

		if ($table_schema is nonnull) {
			$applicable_indexes = $table_schema->indexes;

			if ($table_schema->vitess_sharding) {
				$applicable_indexes[] = new Index(
					$table_schema->vitess_sharding->keyspace,
					'INDEX',
					keyset[$table_schema->vitess_sharding->sharding_key],
				);
			}

			foreach ($filtered_rows as $row_id => $row_to_delete) {
				$index_ref_deletes = self::getIndexModificationsForRow($applicable_indexes, $row_to_delete);

				foreach ($index_ref_deletes as list($index_name, $index_keys, $store_as_unique)) {
					$specific_index_refs = $index_refs[$index_name] ?? null;
					if ($specific_index_refs is nonnull) {
						self::removeFromIndexes(inout $specific_index_refs, $index_keys, $store_as_unique, $row_id);
						$index_refs[$index_name] = $specific_index_refs;
					}
				}
			}
		}

		// write it back to the database
		$conn->getServer()->saveTable($database, $table_name, $remaining_rows, $index_refs);
		return $rows_affected;
	}
}
