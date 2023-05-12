<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Keyset, Str, Vec};

/**
 * An executable Query plan
 *
 * Clause processors used by multiple query types are implemented here
 * Any clause used by only one query type is processed in that subclass
 */
abstract class Query {

	public ?Expression $whereClause = null;
	public ?order_by_clause $orderBy = null;
	public ?limit_clause $limitClause = null;

	/**
	 * The initial query that was executed, no longer needed after parsing but retained for
	 * debugging and logging
	 */
	public string $sql;
	public bool $ignoreDupes = false;

	protected function applyWhere(
		AsyncMysqlConnection $conn,
		dataset $data,
		unique_index_refs $unique_index_refs,
		index_refs $index_refs,
		?dict<string, Column> $columns,
		?vec<Index> $indexes,
	): dataset {
		$where = $this->whereClause;
		if ($where === null) {
			// no where clause? cool! just return the given data
			return $data;
		}

		if ($columns is nonnull && $indexes) {
			$candidates = $where->getIndexCandidates($columns);
			if ($candidates) {
				$candidate_keys = Keyset\keys($candidates);
				$matched_fields = 0;
				$matched_index = null;
				foreach ($indexes as $index) {
					if ($index->fields === $candidate_keys) {
						$matched_index = $index;
						$matched_fields = C\count($index->fields);
						break;
					}

					if (Keyset\intersect($candidate_keys, $index->fields) === $index->fields) {
						$index_field_count = C\count($index->fields);
						if ($index_field_count > $matched_fields) {
							$matched_fields = $index_field_count;
							$matched_index = $index;
						}
					}
				}

				if ($matched_index) {
					if ($matched_fields === 1) {
						$matched_field = vec($matched_index->fields)[0];
						$candidate_key = $candidates[$matched_field] as arraykey;
					} else {
						$candidate_key = '';
						foreach ($matched_index->fields as $matched_field) {
							$candidate_key .= ($candidates[$matched_field] as arraykey).'||';
						}
					}

					$data = self::filterDataWithMatchedIndex(
						$data,
						$unique_index_refs,
						$index_refs,
						$matched_index,
						$candidate_key,
					);
				}
			}
		}

		return Dict\filter($data, $row ==> (bool)$where->evaluate($row, $conn));
	}

	private static function filterDataWithMatchedIndex(
		dataset $data,
		unique_index_refs $unique_index_refs,
		index_refs $index_refs,
		Index $matched_index,
		arraykey $candidate_key,
	): dataset {
		if ($matched_index->type === 'PRIMARY') {
			if (C\contains_key($data, $candidate_key)) {
				return dict[
					$candidate_key => $data[$candidate_key],
				];
			}

			return dict[];
		}

		if ($matched_index->type === 'UNIQUE') {
			if (C\contains_key($unique_index_refs, $matched_index->name)) {
				$matched_index_refs = $unique_index_refs[$matched_index->name];

				if (C\contains_key($matched_index_refs, $candidate_key)) {
					$ref = $matched_index_refs[$candidate_key];
					if (C\contains_key($data, $ref)) {
						return dict[
							$ref => $data[$ref],
						];
					}
				}
			}

			return dict[];
		}

		if ($matched_index->type === 'INDEX') {
			$matched_index_refs = $index_refs[$matched_index->name] ?? null;

			if ($matched_index_refs is nonnull) {
				$refs = $matched_index_refs[$candidate_key] ?? null;
				if ($refs is nonnull) {
					return Dict\filter_with_key($data, ($row_id, $_) ==> C\contains_key($refs, $row_id));
				}
			}

			return dict[];
		}

		throw new \Exception('Unrecognised index');
	}

	/**
	 * Apply the ORDER BY clause to sort the rows
	 */
	protected function applyOrderBy(AsyncMysqlConnection $_conn, dataset $data): dataset {
		$order_by = $this->orderBy;
		if ($order_by === null) {
			return $data;
		}

		// allow all column expressions to fall through to the full row
		foreach ($order_by as $rule) {
			$expr = $rule['expression'];
			if ($expr is ColumnExpression && $expr->tableName === null) {
				$expr->allowFallthrough();
			}
		}

		// sort function applies all ORDER BY criteria to compare two rows
		$sort_fun = (row $a, row $b): int ==> {
			foreach ($order_by as $rule) {
				// in applySelect, the order by expressions are pre-evaluated and saved on the row with their names as keys,
				// so we don't need to evaluate them again here
				$value_a = $a[$rule['expression']->name];
				$value_b = $b[$rule['expression']->name];

				if ($value_a != $value_b) {
					if ($value_a is num && $value_b is num) {
						return (
							((float)$value_a < (float)$value_b ? 1 : 0) ^
							(($rule['direction'] === SortDirection::DESC) ? 1 : 0)
						)
							? -1
							: 1;
					} else {
						return (
							// Use string comparison explicity to handle lexicographical ordering of things like '125' < '5'
							(((Str\compare((string)$value_a, (string)$value_b)) < 0) ? 1 : 0) ^
							(($rule['direction'] === SortDirection::DESC) ? 1 : 0)
						)
							? -1
							: 1;
					}

				}
			}
			return 0;
		};

		// Work around default sorting behavior to provide a usort that looks like MySQL, where equal values are ordered deterministically
		// record the keys in a dict for usort
		$data_temp = dict[];
		$offset = 0;
		foreach ($data as $i => $item) {
			$data_temp[$i] = tuple($i, $offset, $item);
			$offset++;
		}

		$data_temp = Dict\sort($data_temp, (
			(arraykey, int, dict<string, mixed>) $a,
			(arraykey, int, dict<string, mixed>) $b,
		): int ==> {
			$result = $sort_fun($a[2], $b[2]);

			if ($result !== 0) {
				return $result;
			}

			$a_index = $a[1];
			$b_index = $b[1];

			return $b_index > $a_index ? 1 : -1;
		});

		// re-key the input dataset
		$data_temp = vec($data_temp);
		// dicts maintain insert order. the keys will be inserted out of order but have to match the original
		// keys for updates/deletes to be able to delete the right rows
		$data = dict[];
		foreach ($data_temp as $item) {
			$data[$item[0]] = $item[2];
		}

		return $data;
	}

	protected function applyLimit(dataset $data): dataset {
		$limit = $this->limitClause;
		if ($limit === null) {
			return $data;
		}

		// keys in this dict are intentionally out of order if an ORDER BY clause occurred
		// so first we get the ordered keys, then slice that list by the limit clause, then select only those keys
		return Vec\keys($data)
			|> Vec\slice($$, $limit['offset'], $limit['rowcount'])
			|> Dict\select_keys($data, $$);
	}

	/**
	 * Parses a table name that may contain a . to reference another database
	 * Returns the fully qualified database name and table name as a tuple
	 * If there is no ".", the database name will be the connection's current database
	 */
	public static function parseTableName(AsyncMysqlConnection $conn, string $table): (string, string) {
		// referencing a table from another database on the same server?
		if (Str\contains($table, '.')) {
			$parts = Str\split($table, '.');
			if (C\count($parts) !== 2) {
				throw new SQLFakeRuntimeException("Table name $table has too many parts");
			}
			list($database, $table_name) = $parts;
			return tuple($database, $table_name);
		} else {
			// otherwise use connection context's database
			$database = $conn->getDatabase();
			return tuple($database, $table);
		}
	}

	/**
	 * Apply the "SET" clause of an UPDATE, or "ON DUPLICATE KEY UPDATE"
	 */
	protected function applySet(
		AsyncMysqlConnection $conn,
		string $database,
		string $table_name,
		dataset $filtered_rows,
		dataset $original_table,
		unique_index_refs $unique_index_refs,
		index_refs $index_refs,
		vec<BinaryOperatorExpression> $set_clause,
		?TableSchema $table_schema,
		/* for dupe inserts only */
		?row $values = null,
	): (int, dataset, unique_index_refs, index_refs) {
		$valid_fields = null;
		if ($table_schema !== null) {
			$valid_fields = Keyset\map($table_schema->fields, $field ==> $field->name);
		}

		$columns = keyset[];
		$set_clauses = vec[];
		foreach ($set_clause as $expression) {
			// the parser already asserts this at parse time
			$left = $expression->left as ColumnExpression;
			$right = $expression->right as nonnull;
			$column = $left->name;
			$columns[] = $column;

			// If we know the valid fields for this table, only allow setting those
			if ($valid_fields !== null) {
				if (!C\contains($valid_fields, $column)) {
					throw new SQLFakeRuntimeException("Invalid update column {$column}");
				}
			}

			$set_clauses[] = shape('column' => $column, 'expression' => $right);
		}

		$applicable_indexes = vec[];

		if ($table_schema is nonnull) {
			foreach ($table_schema->indexes as $index) {
				if (Keyset\intersect($index->fields, $columns) !== keyset[]) {
					$applicable_indexes[] = $index;
				}
			}
		}

		$update_count = 0;

		foreach ($filtered_rows as $row_id => $row) {
			$changes_found = false;

			// a copy of the $row to be updated
			$update_row = $row;
			if ($values is nonnull) {
				// this is a bit of a hack to make the VALUES() function work without changing the
				// interface of all ->evaluate() expressions to include the values list as well
				// we put the values on the row as though they were another table
				// we do this on a copy so that we don't accidentally save these to the table
				foreach ($values as $col => $val) {
					$update_row['sql_fake_values.'.$col] = $val;
				}
			}

			list($unique_index_ref_deletes, $index_ref_deletes) = self::getIndexRemovalsForRow(
				$applicable_indexes,
				$row_id,
				$row,
			);

			foreach ($set_clauses as $clause) {
				$existing_value = $row[$clause['column']] ?? null;
				$expr = $clause['expression'];
				$new_value = $expr->evaluate($update_row, $conn);

				if ($new_value !== $existing_value) {
					$row[$clause['column']] = $new_value;
					$changes_found = true;
				}
			}

			$new_row_id = $row_id;
			$unique_index_ref_additions = vec[];
			$index_ref_additions = vec[];

			if ($changes_found) {
				if ($table_schema is nonnull) {
					// throw on invalid data types if strict mode
					$row = DataIntegrity::coerceToSchema($row, $table_schema);
				}

				foreach ($applicable_indexes as $index) {
					if ($index->type === 'PRIMARY') {
						if (C\count($index->fields) === 1) {
							$index_key = $row[C\firstx($index->fields)] as arraykey;
						} else {
							$index_key = '';
							foreach ($index->fields as $field) {
								$index_key .= $row[$field] as arraykey.'||';
							}
						}

						$new_row_id = $index_key;
					}
				}

				list($unique_index_ref_additions, $index_ref_additions) = self::getIndexAdditionsForRow(
					$applicable_indexes,
					$row,
				);
			}

			if ($changes_found) {
				if ($table_schema is nonnull) {
					$key_violation = false;

					if (C\contains_key($original_table, $new_row_id)) {
						$key_violation = true;
					} else {
						foreach ($unique_index_ref_deletes as list($index_name, $index_key)) {
							if (
								isset($unique_index_refs[$index_name][$index_key]) &&
								$unique_index_refs[$index_name][$index_key] !== $row_id
							) {
								$key_violation = true;
								break;
							}
						}
					}

					$result = null;
					if ($key_violation) {
						$result = DataIntegrity::checkUniqueConstraints($original_table, $row, $table_schema, $row_id);
					}

					if ($result is nonnull) {
						if ($this->ignoreDupes) {
							continue;
						}
						if (!QueryContext::$relaxUniqueConstraints) {
							throw new SQLFakeUniqueKeyViolation($result[0]);
						}
					}
				}

				foreach ($unique_index_ref_deletes as list($table, $key)) {
					unset($unique_index_refs[$table][$key]);
				}

				foreach ($index_ref_deletes as list($table, $key, $index_row)) {
					unset($index_refs[$table][$key][$index_row]);
				}

				foreach ($unique_index_ref_additions as list($index_name, $index_key)) {
					if (!C\contains_key($unique_index_refs, $index_name)) {
						$unique_index_refs[$index_name] = dict[];
					}
					$unique_index_refs[$index_name][$index_key] = $new_row_id;
				}

				foreach ($index_ref_additions as list($index_name, $index_key)) {
					if (!C\contains_key($index_refs, $index_name)) {
						$index_refs[$index_name] = dict[];
					}
					if (!C\contains_key($index_refs[$index_name], $index_key)) {
						$index_refs[$index_name][$index_key] = keyset[];
					}
					$index_refs[$index_name][$index_key][] = $new_row_id;
				}

				if ($new_row_id !== $row_id) {
					// Remap keys to preserve insertion order when primary key has changed
					$original_table = Dict\pull_with_key(
						$original_table,
						($k, $v) ==> $k === $row_id ? $row : $v,
						($k, $_) ==> $k === $row_id ? $new_row_id : $k,
					);
				} else {
					$original_table[$row_id] = $row;
				}

				$update_count++;
			}
		}

		// write it back to the database
		$conn->getServer()->saveTable($database, $table_name, $original_table, $unique_index_refs, $index_refs);
		return tuple($update_count, $original_table, $unique_index_refs, $index_refs);
	}

	public static function getIndexRemovalsForRow(
		vec<Index> $applicable_indexes,
		arraykey $row_id,
		row $row,
	): (vec<(string, arraykey)>, vec<(string, arraykey, arraykey)>) {
		$unique_index_ref_deletes = vec[];
		$index_ref_deletes = vec[];

		foreach ($applicable_indexes as $index) {
			if (C\count($index->fields) === 1) {
				$index_key = $row[C\firstx($index->fields)] as ?arraykey;
			} else {
				$index_key = '';
				$saw_null = false;

				foreach ($index->fields as $field) {
					$index_part = $row[$field] as ?arraykey;

					if ($index_part is null) {
						$saw_null = true;
						break;
					}

					$index_key .= $row[$field] as arraykey.'||';
				}

				if ($saw_null) {
					$index_key = null;
				}
			}

			if ($index_key is null) {
				continue;
			}

			if ($index->type === 'UNIQUE') {
				$unique_index_ref_deletes[] = tuple($index->name, $index_key);
			} else if ($index->type === 'INDEX') {
				$index_ref_deletes[] = tuple($index->name, $index_key, $row_id);
			}
		}

		return tuple($unique_index_ref_deletes, $index_ref_deletes);
	}

	public static function getIndexAdditionsForRow(
		vec<Index> $applicable_indexes,
		row $row,
	): (vec<(string, arraykey)>, vec<(string, arraykey)>) {
		$unique_index_ref_additions = vec[];
		$index_ref_additions = vec[];

		foreach ($applicable_indexes as $index) {
			if (C\count($index->fields) === 1) {
				$index_key = $row[C\firstx($index->fields)] as ?arraykey;
			} else {
				$index_key = '';
				$saw_null = false;

				foreach ($index->fields as $field) {
					$index_part = $row[$field] as ?arraykey;

					if ($index_part is null) {
						$saw_null = true;
						break;
					}

					$index_key .= $row[$field] as arraykey.'||';
				}

				if ($saw_null) {
					$index_key = null;
				}
			}

			if ($index_key is null) {
				continue;
			}

			if ($index->type === 'UNIQUE') {
				$unique_index_ref_additions[] = tuple($index->name, $index_key);
			} else if ($index->type === 'INDEX') {
				$index_ref_additions[] = tuple($index->name, $index_key);
			}
		}

		return tuple($unique_index_ref_additions, $index_ref_additions);
	}
}
