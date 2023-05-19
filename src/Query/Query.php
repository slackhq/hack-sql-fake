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
			$all_matched = false;
			$data = self::filterWithIndexes($data, $index_refs, $columns, $indexes, $where, inout $all_matched);

			if ($all_matched) {
				return $data;
			}
		}

		return Dict\filter($data, $row ==> (bool)$where->evaluate($row, $conn));
	}

	private static function filterWithIndexes(
		dataset $data,
		index_refs $index_refs,
		dict<string, Column> $columns,
		vec<Index> $indexes,
		Expression $where,
		inout bool $all_matched,
	): dataset {
		$ored_wheres = self::getOredExpressions($where);

		$data_keys = Keyset\keys($data);

		if (C\count($ored_wheres) === 1) {
			$matched_all_expressions = true;
			$candidates = self::getIndexCandidates($where, $columns, inout $matched_all_expressions);
			if ($candidates) {
				$all_expressions_indexed = true;
				$filtered_keys = self::getKeysForConditional(
					$data_keys,
					$index_refs,
					$indexes,
					$candidates,
					inout $all_expressions_indexed,
				);

				if ($filtered_keys is nonnull) {
					$data = Dict\filter_keys($data, $row_pk ==> C\contains_key($filtered_keys, $row_pk));
					if ($matched_all_expressions) {
						if ($all_expressions_indexed) {
							$all_matched = true;
						}

						return $data;
					}
				}
			}
		} else {
			// calculating merged index

			$all_filtered_keys = keyset[];

			$can_filter = true;

			foreach ($ored_wheres as $ored_where) {
				$matched_all_expressions = true;
				$candidates = self::getIndexCandidates($ored_where, $columns, inout $matched_all_expressions);

				if ($candidates) {
					$filtered_keys = self::getKeysForConditional(
						$data_keys,
						$index_refs,
						$indexes,
						$candidates,
						inout $matched_all_expressions,
					);

					if ($filtered_keys is nonnull) {
						$all_filtered_keys = Keyset\union($all_filtered_keys, $filtered_keys);
					} else {
						$can_filter = false;
						break;
					}
				} else {
					$can_filter = false;
					break;
				}
			}

			if ($can_filter) {
				return Dict\filter_keys($data, $row_pk ==> C\contains_key($all_filtered_keys, $row_pk));
			}
		}

		return $data;
	}

	private static function getKeysForConditional(
		keyset<arraykey> $data_keys,
		index_refs $index_refs,
		vec<Index> $indexes,
		dict<string, vec<mixed>> $candidates,
		inout bool $all_expressions_indexed,
	): ?keyset<arraykey> {
		$candidate_keys = Keyset\keys($candidates);
		$matched_fields = 0;
		$matched_index = null;
		$has_multiple_fields = false;

		foreach ($indexes as $index) {
			$index_field_count = C\count($index->fields);

			if ($index->fields === $candidate_keys) {
				$matched_index = $index;
				$matched_fields = C\count($index->fields);
				break;
			}

			if (C\count(Keyset\intersect($candidate_keys, $index->fields)) === $index_field_count) {
				if ($index_field_count > $matched_fields) {
					$matched_fields = $index_field_count;
					$matched_index = $index;
				}
			}

			if ($index_field_count > 1) {
				$has_multiple_fields = true;
			}
		}

		if ($matched_index) {
			if (C\count($candidates) > $matched_fields) {
				$all_expressions_indexed = false;
			}

			return self::filterDataWithMatchedIndex(
				$data_keys,
				$index_refs,
				$matched_index,
				$matched_fields,
				$candidates,
				true,
			);
		} else {
			$all_expressions_indexed = false;

			if ($has_multiple_fields) {
				foreach ($indexes as $index) {
					if (C\count($index->fields) > 1) {
						$partial_index_candidate_fields = keyset[];

						foreach ($index->fields as $field) {
							if (C\contains_key($candidate_keys, $field)) {
								$partial_index_candidate_fields[] = $field;
							} else {
								break;
							}
						}

						if ($partial_index_candidate_fields) {
							if (C\count($partial_index_candidate_fields) >= $matched_fields) {
								$matched_fields = C\count($partial_index_candidate_fields);
								$matched_index = new Index($index->name, $index->type, $partial_index_candidate_fields);
							}
						}
					}
				}
			}

			if ($matched_index) {
				return self::filterDataWithMatchedIndex(
					$data_keys,
					$index_refs,
					$matched_index,
					$matched_fields,
					$candidates,
					false,
				);
			} else if (!QueryContext::$relaxUniqueConstraints) {
				//throw new \Exception('Query without index');
			}

			return null;
		}
	}

	private static function getOredExpressions(Expression $expr): vec<Expression> {
		if (!self::containsOrs($expr)) {
			return vec[$expr];
		}

		$expr as BinaryOperatorExpression;

		if ($expr->negated) {
			return vec[$expr];
		}

		if ($expr->operator === Operator::AND) {
			$left_ored_exprs = self::getOredExpressions($expr->left);
			$right_ored_exprs = self::getOredExpressions($expr->right as nonnull);

			$all_ored_exprs = vec[];

			if (C\count($left_ored_exprs) > 1 || C\count($right_ored_exprs) > 1) {
				foreach ($left_ored_exprs as $left_ored_expr) {
					foreach ($right_ored_exprs as $right_ored_expr) {
						$all_ored_exprs[] = new BinaryOperatorExpression(
							$left_ored_expr,
							false,
							Operator::AND,
							$right_ored_expr,
						);
					}
				}
			} else {
				$all_ored_exprs[] = $expr;
			}

			return $all_ored_exprs;
		}

		if ($expr->operator === Operator::OR) {
			$left_ored_exprs = self::getOredExpressions($expr->left);
			$right_ored_exprs = self::getOredExpressions($expr->right as nonnull);

			return Vec\concat($left_ored_exprs, $right_ored_exprs);
		}

		return vec[];
	}

	private static function containsOrs(Expression $expr): bool {
		if (!$expr is BinaryOperatorExpression) {
			return false;
		}

		if ($expr->operator === Operator::OR) {
			return true;
		}

		if ($expr->operator === Operator::AND) {
			return self::containsOrs($expr->left) || self::containsOrs($expr->right as nonnull);
		}

		return false;
	}

	private static function getIndexCandidates(
		Expression $expr,
		dict<string, Column> $columns,
		inout bool $matched_all_expressions,
	): dict<string, vec<mixed>> {
		if ($expr is BinaryOperatorExpression) {
			return self::getIndexCandidatesFromBinop($expr, $columns, inout $matched_all_expressions);
		}

		if ($expr is InOperatorExpression && !$expr->negated) {
			if ($expr->left is ColumnExpression) {
				$column_names = dict[];

				$table_name = $expr->left->tableName;
				$column_name = $expr->left->name;

				if ($table_name is null) {
					$dot_column_name = '.'.$column_name;
					foreach ($columns as $key => $_) {
						if (Str\ends_with($key, $dot_column_name)) {
							$table_name = Str\slice($key, 0, Str\length($key) - Str\length($dot_column_name));
						}
					}
				}

				if ($table_name is nonnull) {
					$column_name = $table_name.'.'.$column_name;
				}

				$values = vec[];

				foreach (($expr->inList as nonnull) as $in_expr) {
					// found it? return the opposite of "negated". so if negated is false, return true.
					// if it's a subquery, we have to iterate over the results and extract the field from each row
					if ($in_expr is ConstantExpression) {
						$value = $in_expr->value;
						if (isset($columns[$column_name])) {
							if ($columns[$column_name]->hack_type === 'int' && $value is string) {
								$value = (int)$value;
							}
						}
						$values[] = $value;
					} else {
						$values = vec[];
						break;
					}
				}

				if ($values) {
					$column_names[$column_name] = $values;

					return $column_names;
				}
			}
		}

		$matched_all_expressions = false;

		return dict[];
	}

	private static function getIndexCandidatesFromBinop(
		BinaryOperatorExpression $expr,
		dict<string, Column> $columns,
		inout bool $matched_all_expressions,
	): dict<string, vec<mixed>> {
		$column_names = dict[];

		if ($expr->operator === null) {
			// an operator should only be in this state in the middle of parsing, never when evaluating
			throw new SQLFakeRuntimeException('Attempted to evaluate BinaryOperatorExpression with empty operator');
		}

		if ($expr->negated) {
			$matched_all_expressions = false;
			return dict[];
		}

		if ($expr->operator === Operator::EQUALS) {
			if ($expr->left is ColumnExpression && $expr->left->name !== '*' && $expr->right is ConstantExpression) {
				$table_name = $expr->left->tableName;
				$column_name = $expr->left->name;

				if ($table_name is null) {
					$dot_column_name = '.'.$column_name;
					foreach ($columns as $key => $_) {
						if (Str\ends_with($key, $dot_column_name)) {
							$table_name = Str\slice($key, 0, Str\length($key) - Str\length($dot_column_name));
						}
					}
				}

				if ($table_name is nonnull) {
					$column_name = $table_name.'.'.$column_name;
				}

				$value = $expr->right->value;
				if (isset($columns[$column_name])) {
					if ($columns[$column_name]->hack_type === 'int') {
						$value = (int)$value;
					}
				}
				$column_names[$column_name] = vec[$value];
			}

			$matched_all_expressions = false;

			return $column_names;
		}

		if ($expr->operator === Operator::AND) {
			$column_names = self::getIndexCandidates($expr->left, $columns, inout $matched_all_expressions);
			$column_names = Dict\merge(
				$column_names,
				self::getIndexCandidates($expr->right as nonnull, $columns, inout $matched_all_expressions),
			);

			return $column_names;
		}

		$matched_all_expressions = false;

		return $column_names;
	}

	private static function filterDataWithMatchedIndex(
		keyset<arraykey> $data_keys,
		index_refs $index_refs,
		Index $matched_index,
		int $matched_fields,
		dict<string, vec<mixed>> $candidates,
		bool $full_index,
	): keyset<arraykey> {
		if ($matched_fields === 1) {
			$keys = keyset[];

			$matched_field = C\firstx($matched_index->fields);
			foreach ($candidates[$matched_field] as $candidate_value) {
				if ($candidate_value is null) {
					$candidate_value = '__NULL__';
				}
				$keys[] = $candidate_value as arraykey;
			}
		} else {
			$keys = vec[];

			foreach ($matched_index->fields as $matched_field) {
				if ($keys === vec[]) {
					foreach ($candidates[$matched_field] as $candidate_value) {
						$keys[] = vec[$candidate_value as arraykey];
					}
				} else {
					$new_keys = vec[];

					foreach ($keys as $key_parts) {
						foreach ($candidates[$matched_field] as $candidate_value) {
							if ($candidate_value is null) {
								$candidate_value = '__NULL__';
							}
							$new_keys[] = Vec\concat($key_parts, vec[$candidate_value as arraykey]);
						}
					}

					$keys = $new_keys;
				}
			}
		}

		if ($matched_index->type === 'PRIMARY' && $keys is keyset<_> && $full_index) {
			// this is the happiest path - a simple primary key lookup on an index with a single column
			return Keyset\intersect($data_keys, $keys);
		}

		// for unique indexes and primary keys with more than one field we store indexes
		// as nested dicts based on their fields
		if ($matched_index->type === 'UNIQUE' || $matched_index->type === 'PRIMARY') {
			$matched_index_refs = $index_refs[$matched_index->name] ?? null;

			if ($matched_index_refs is null) {
				return keyset[];
			}

			if ($keys is keyset<_>) {
				if ($full_index) {
					// this is the second-happiest-path — a unique index with a single column
					return keyset<arraykey>(
						Dict\filter_keys(
						/* HH_FIXME[4110] */
						$matched_index_refs,
							$ref_k ==> C\contains_key($keys, $ref_k),
						),
					);
				}

				// we have a partial index lookup, which means we need to filter on those keys then collapse the result
				return self::collapseRefs(
					Dict\filter_keys($matched_index_refs, $ref_k ==> C\contains_key($keys, $ref_k)),
				);
			}

			$matched_keys = keyset[];

			// this is a full or partial index lookup with multiple fields
			foreach ($keys as $key_parts) {
				$key_matched_index_refs = $matched_index_refs;

				foreach ($key_parts as $i => $key_part) {
					$next = $key_matched_index_refs[$key_part] ?? null;

					if ($next is null) {
						break;
					}

					if ($i + 1 === $matched_fields) {
						if (!$full_index) {
							$matched_keys = Keyset\union($matched_keys, self::collapseRefs($next as dict<_, _>));
						} else {
							// for unique indexes this is always an arraykey
							$matched_keys[] = $next as arraykey;
						}
						break;
					}

					$key_matched_index_refs = $next as dict<_, _>;
				}
			}

			return $matched_keys;
		}

		if ($matched_index->type === 'INDEX') {
			$matched_index_refs = $index_refs[$matched_index->name] ?? null;

			if ($matched_index_refs is null) {
				return keyset[];
			}

			if ($keys is keyset<_>) {
				if ($full_index) {
					// a non-unique index with a single column
					return keyset<arraykey>(
						Dict\flatten(
							Dict\filter_keys(
								/* HH_FIXME[4110] */
								$matched_index_refs,
								$ref_k ==> C\contains_key($keys, $ref_k),
							),
						),
					);
				}

				// we have a partial index lookup, which means we need to filter on those keys then collapse the result
				return self::collapseRefs(
					Dict\filter_keys($matched_index_refs, $ref_k ==> C\contains_key($keys, $ref_k)),
				);
			}

			$matched_keys = keyset[];

			// this is a full or partial index lookup with multiple fields
			foreach ($keys as $key_parts) {
				$key_matched_index_refs = $matched_index_refs;
				foreach ($key_parts as $i => $key_part) {
					$next = $key_matched_index_refs[$key_part] ?? null;

					if ($next is null) {
						break;
					}

					if ($i + 1 === $matched_fields) {
						if (!$full_index) {
							$matched_keys = Keyset\union($matched_keys, self::collapseRefs($next as dict<_, _>));
						} else {
							// for non-unique indexes this is always a keyset
							$matched_keys = Keyset\union($matched_keys, $next as keyset<_>);
						}
						break;
					}

					$key_matched_index_refs = $next as dict<_, _>;
				}
			}

			return $matched_keys;

			return keyset[];
		}

		throw new \Exception('Unrecognised index');
	}

	private static function collapseRefs(dict<arraykey, mixed> $refs): keyset<arraykey> {
		$out = keyset[];

		foreach ($refs as $value) {
			if ($value is dict<_, _>) {
				$out = Keyset\union($out, self::collapseRefs($value));
			} else if ($value is keyset<_>) {
				$out = Keyset\union($out, $value);
			} else if ($value is arraykey) {
				$out[] = $value;
			}
		}

		return $out;
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
		index_refs $index_refs,
		vec<BinaryOperatorExpression> $set_clause,
		?TableSchema $table_schema,
		/* for dupe inserts only */
		?row $values = null,
	): (int, dataset, index_refs) {
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

		$primary_key_columns = $table_schema?->getPrimaryKeyColumns() ?? keyset[];
		$primary_key_changed = false;

		foreach ($set_clauses as $clause) {
			if (C\contains_key($primary_key_columns, $clause['column'])) {
				$primary_key_changed = true;
			}
		}

		$applicable_indexes = vec[];

		if ($table_schema is nonnull) {
			foreach ($table_schema->indexes as $index) {
				if ($primary_key_changed || Keyset\intersect($index->fields, $columns) !== keyset[]) {
					$applicable_indexes[] = $index;
				}
			}

			if ($table_schema->vitess_sharding) {
				$applicable_indexes[] = new Index(
					$table_schema->vitess_sharding->keyspace,
					'INDEX',
					keyset[$table_schema->vitess_sharding->sharding_key],
				);
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

			$index_ref_deletes = self::getIndexModificationsForRow($applicable_indexes, $row);

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
			$index_ref_additions = vec[];

			if ($changes_found) {
				if ($table_schema is nonnull) {
					// throw on invalid data types if strict mode
					$row = DataIntegrity::coerceToSchema($row, $table_schema);
				}

				foreach ($applicable_indexes as $index) {
					if ($index->type === 'PRIMARY' && C\count($index->fields) === 1) {
						$new_row_id = $row[C\firstx($index->fields)] as arraykey;
						break;
					}
				}

				$index_ref_additions = self::getIndexModificationsForRow($applicable_indexes, $row);
			}

			if ($changes_found) {
				if ($table_schema is nonnull) {
					$key_violation = false;

					if (C\contains_key($original_table, $new_row_id)) {
						$key_violation = true;
					} else {
						foreach ($index_ref_deletes as list($index_name, $index_keys, $store_as_unique)) {
							if ($store_as_unique) {
								$leaf = $index_refs[$index_name] ?? null;

								foreach ($index_keys as $index_key) {
									$leaf = $leaf[$index_key] ?? null;

									if ($leaf is null) {
										break;
									}

									if ($leaf is arraykey && $leaf !== $row_id) {
										$key_violation = true;
										break;
									}
								}
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

				foreach ($index_ref_deletes as list($index_name, $index_keys, $store_as_unique)) {
					$specific_index_refs = $index_refs[$index_name] ?? null;
					if ($specific_index_refs is nonnull) {
						self::removeFromIndexes(inout $specific_index_refs, $index_keys, $store_as_unique, $row_id);
						$index_refs[$index_name] = $specific_index_refs;
					}
				}

				foreach ($index_ref_additions as list($index_name, $index_keys, $store_as_unique)) {
					$specific_index_refs = $index_refs[$index_name] ?? dict[];
					self::addToIndexes(inout $specific_index_refs, $index_keys, $store_as_unique, $new_row_id);
					$index_refs[$index_name] = $specific_index_refs;
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
		$conn->getServer()->saveTable($database, $table_name, $original_table, $index_refs);
		return tuple($update_count, $original_table, $index_refs);
	}

	public static function getIndexModificationsForRow(
		vec<Index> $applicable_indexes,
		row $row,
	): vec<(string, vec<arraykey>, bool)> {
		$index_ref_deletes = vec[];

		foreach ($applicable_indexes as $index) {
			if ($index->type === 'PRIMARY' && C\count($index->fields) === 1) {
				continue;
			}

			$store_as_unique = $index->type === 'UNIQUE' || $index->type === 'PRIMARY';

			$index_field_count = C\count($index->fields);

			if ($index_field_count === 1) {
				$index_part = $row[C\firstx($index->fields)] as ?arraykey;

				if ($index_part is null) {
					$index_part = '__NULL__';
				}

				$index_key = vec[$index_part];
			} else {
				$index_key = vec[];

				$inc = 0;

				foreach ($index->fields as $field) {
					$index_part = $row[$field] as ?arraykey;

					if ($index_part is null) {
						$index_part = '__NULL__';

						// don't store unique indexes with null
						if ($index->type === 'UNIQUE' && $inc < $index_field_count - 1) {
							if ($inc > 0) {
								$store_as_unique = false;
							}

							break;
						}
					}

					$index_key[] = $index_part;

					$inc++;
				}

				// this happens if the first index column contains a null value — in which case
				// we don't store anything
				if ($index_key === vec[]) {
					continue;
				}
			}

			$index_ref_deletes[] = tuple($index->name, $index_key, $store_as_unique);
		}

		return $index_ref_deletes;
	}

	/**
	 * This is an ugly, ugly method — but I believe it's the only way to achieve this in Hack
	 */
	public static function removeFromIndexes(
		inout dict<arraykey, mixed> $index_refs,
		vec<arraykey> $index_keys,
		bool $store_as_unique,
		arraykey $row_id,
	): void {
		$key_length = C\count($index_keys);

		if ($key_length === 1) {
			if ($store_as_unique) {
				unset($index_refs[$index_keys[0]]);
			} else {
				/* HH_FIXME[4135] */
				unset(
					/* HH_FIXME[4063] */
					$index_refs[$index_keys[0]][$row_id]
				);
			}
		} else {
			$nested_indexes = $index_refs[$index_keys[0]] ?? null;

			if ($nested_indexes is dict<_, _>) {
				self::removeFromIndexes(inout $nested_indexes, Vec\drop($index_keys, 1), $store_as_unique, $row_id);

				if ($nested_indexes) {
					$index_refs[$index_keys[0]] = $nested_indexes;
				} else {
					unset($index_refs[$index_keys[0]]);
				}
			}

		}
	}

	/**
	 * This is an ugly, ugly method — but I believe it's the only way to achieve this in Hack
	 */
	public static function addToIndexes(
		inout dict<arraykey, mixed> $index_refs,
		vec<arraykey> $index_keys,
		bool $store_as_unique,
		arraykey $row_id,
	): void {
		$key_length = C\count($index_keys);

		if ($key_length === 1) {
			if ($store_as_unique) {
				$index_refs[$index_keys[0]] = $row_id;
			} else {
				$index_refs[$index_keys[0]] ??= keyset[];
				/* HH_FIXME[4006] */
				$index_refs[$index_keys[0]][] = $row_id;
			}
		} else if ($key_length > 1) {
			$nested_indexes = $index_refs[$index_keys[0]] ?? dict[];

			if ($nested_indexes is dict<_, _>) {
				self::addToIndexes(inout $nested_indexes, Vec\drop($index_keys, 1), $store_as_unique, $row_id);

				$index_refs[$index_keys[0]] = $nested_indexes;
			}
		}
	}
}
