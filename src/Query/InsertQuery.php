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
		list($table, $unique_index_refs, $index_refs) =
			$conn->getServer()->getTableData($database, $table_name) ?? tuple(dict[], dict[], dict[]);

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

			if ($primary_key_columns is nonnull) {
				if (C\count($primary_key_columns) === 1) {
					$primary_key = $row[C\firstx($primary_key_columns)] as arraykey;
				} else {
					$primary_key = '';
					foreach ($primary_key_columns as $primary_key_column) {
						$primary_key .= (string)$row[$primary_key_column].'||';
					}
				}
			} else {
				$primary_key = C\count($table);
			}

			$unique_index_ref_additions = vec[];
			$index_ref_additions = vec[];

			list($unique_index_ref_additions, $index_ref_additions) =
				self::getIndexAdditionsForRow($table_schema->indexes, $row);

			$key_violation = false;

			if (isset($table[$primary_key])) {
				$key_violation = true;
			} else {
				foreach ($unique_index_ref_additions as list($index_name, $index_key)) {
					if (
						isset($unique_index_refs[$index_name][$index_key]) &&
						$unique_index_refs[$index_name][$index_key] !== $primary_key
					) {
						$key_violation = true;
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
					list($affected, $table, $unique_index_refs, $index_refs) = $this->applySet(
						$conn,
						$database,
						$table_name,
						dict[$row_id => $existing_row],
						$table,
						$unique_index_refs,
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

			foreach ($unique_index_ref_additions as list($index_name, $index_key)) {
				if (!C\contains_key($unique_index_refs, $index_name)) {
					$unique_index_refs[$index_name] = dict[];
				}
				$unique_index_refs[$index_name][$index_key] = $primary_key;
			}

			foreach ($index_ref_additions as list($index_name, $index_key)) {
				if (!C\contains_key($index_refs, $index_name)) {
					$index_refs[$index_name] = dict[];
				}
				if (!C\contains_key($index_refs[$index_name], $index_key)) {
					$index_refs[$index_name][$index_key] = keyset[];
				}
				$index_refs[$index_name][$index_key][] = $primary_key;
			}

			$table[$primary_key] = $row;
			$rows_affected++;
		}

		// write it back to the database
		$conn->getServer()->saveTable($database, $table_name, $table, $unique_index_refs, $index_refs);
		return $rows_affected;
	}
}
