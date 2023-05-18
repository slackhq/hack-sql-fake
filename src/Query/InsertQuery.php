<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

final class InsertQuery extends Query {

	public function __construct(public string $table, public string $sql, public bool $ignoreDupes) {}

	public vec<BinaryOperatorExpression> $updateExpressions = vec[];
	public vec<string> $insertColumns = vec[];
	public vec<vec<Expression>> $values = vec[];

	/**
	 * Insert rows, with validation
	 * Returns number of rows affected
	 */
	public function execute(AsyncMysqlConnection $conn): int {
		list($database, $table_name) = Query::parseTableName($conn, $this->table);
		list($table, $index_refs) = $conn->getServer()->getTableData($database, $table_name) ?? tuple(dict[], dict[]);

		Metrics::trackQuery(QueryType::INSERT, $conn->getServer()->name, $table_name, $this->sql);

		$table_schema = QueryContext::getSchema($database, $table_name);
		if ($table_schema === null && QueryContext::$strictSchemaMode) {
			throw new SQLFakeRuntimeException("Table $table_name not found in schema and strict mode is enabled");
		}

		$rows_affected = 0;
		foreach ($this->values as $value_list) {
			$row = dict[];
			foreach ($this->insertColumns as $key => $col) {
				$row[$col] = $value_list[$key]->evaluate(dict[], $conn);
			}

			// can't enforce uniqueness or defaults if there is no schema available
			if ($table_schema === null) {
				$table[C\count($table)] = $row;
				$rows_affected++;
				continue;
			}

			// ensure all fields are present with appropriate types and default values
			// throw for nonexistent fields
			$row = DataIntegrity::coerceToSchema($row, $table_schema);

			$primary_key_columns = $table_schema->getPrimaryKeyColumns();

			if ($primary_key_columns is nonnull && C\count($primary_key_columns) === 1) {
				$primary_key = $row[C\firstx($primary_key_columns)] as arraykey;
			} else {
				// for primary key columns that span multiple columns, we store the
				// index separately and reference an integer count
				$primary_key = C\count($table);
			}

			$index_ref_additions = vec[];

			$applicable_indexes = $table_schema->indexes;

			if ($table_schema->vitess_sharding) {
				$applicable_indexes[] = new Index(
					$table_schema->vitess_sharding->keyspace,
					'INDEX',
					keyset[$table_schema->vitess_sharding->sharding_key],
				);
			}

			$index_ref_additions = self::getIndexModificationsForRow($applicable_indexes, $row);

			$key_violation = false;

			if (isset($table[$primary_key])) {
				$key_violation = true;
			} else {
				foreach ($index_ref_additions as list($index_name, $index_keys, $store_as_unique)) {
					if ($store_as_unique) {
						$leaf = $index_refs[$index_name] ?? null;

						foreach ($index_keys as $index_key) {
							$leaf = $leaf[$index_key] ?? null;

							if ($leaf is null) {
								break;
							}

							if ($leaf is arraykey && $leaf !== $primary_key) {
								$key_violation = true;
								break;
							}
						}
					}
				}
			}

			$unique_key_violation = null;
			if ($key_violation) {
				$unique_key_violation = DataIntegrity::checkUniqueConstraints($table, $row, $table_schema);
			}

			if ($unique_key_violation is nonnull) {
				list($msg, $row_id) = $unique_key_violation;
				// is this an "INSERT ... ON DUPLICATE KEY UPDATE?"
				// if so, this is where we apply the updates
				if (!C\is_empty($this->updateExpressions)) {
					$existing_row = $table[$row_id];
					list($affected, $table, $index_refs) = $this->applySet(
						$conn,
						$database,
						$table_name,
						dict[$row_id => $existing_row],
						$table,
						$index_refs,
						$this->updateExpressions,
						$table_schema,
						$row,
					);
					// MySQL always counts dupe inserts twice intentionally
					$rows_affected += $affected * 2;
					continue;
				}

				if (!$this->ignoreDupes && !QueryContext::$relaxUniqueConstraints) {
					throw new SQLFakeUniqueKeyViolation($msg);
				}

				continue;
			}

			foreach ($index_ref_additions as list($index_name, $index_keys, $store_as_unique)) {
				$specific_index_refs = $index_refs[$index_name] ?? dict[];
				self::addToIndexes(inout $specific_index_refs, $index_keys, $store_as_unique, $primary_key);
				$index_refs[$index_name] = $specific_index_refs;
			}

			$table[$primary_key] = $row;
			$rows_affected++;
		}

		// write it back to the database
		$conn->getServer()->saveTable($database, $table_name, $table, $index_refs);
		return $rows_affected;
	}
}
