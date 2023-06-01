<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Keyset, Vec};

/**
 * Represents the entire FROM clause of a query,
 * built up incrementally when parsing.
 *
 * Contains zero or more from_table expressions, order matters
 */
final class FromClause {

	public vec<from_table> $tables = vec[];
	public bool $mostRecentHasAlias = false;

	public function addTable(from_table $table): void {
		$this->tables[] = $table;
		$this->mostRecentHasAlias = false;
	}

	public function aliasRecentExpression(string $name): void {
		$k = C\last_key($this->tables);
		if ($k === null || $this->mostRecentHasAlias) {
			throw new SQLFakeParseException('Unexpected AS');
		}
		$this->tables[$k]['alias'] = $name;
		$this->mostRecentHasAlias = true;
	}

	/**
	 * The FROM clause of the query gets processed first, retrieving data from tables, executing subqueries, and handling joins
	 * This is also where we build up the $columns list which is commonly used throughout the entire library to map column references to index_refs in this dataset
	 * @reviewer, we don't build up the $columns, since the variable is unused...
	 */
	public function process(
		AsyncMysqlConnection $conn,
		string $sql,
	): (dataset, index_refs, keyset<arraykey>, vec<Index>, dict<string, Column>) {

		$data = dict[];
		$is_first_table = true;
		$index_refs = dict[];
		$dirty_pks = keyset[];

		$indexes = vec[];
		$columns = dict[];

		foreach ($this->tables as $table) {
			$schema = null;
			$new_index_refs = dict[];
			$new_dirty_pks = keyset[];
			$new_indexes = vec[];

			if (Shapes::keyExists($table, 'subquery')) {
				$res = $table['subquery']->evaluate(dict[], $conn);
				invariant($res is KeyedContainer<_, _>, 'evaluated result of SubqueryExpression must be dataset');
				$name = $table['name'];
			} else {
				$table_name = $table['name'];

				list($database, $table_name) = Query::parseTableName($conn, $table_name);

				// TODO if different database, should $name have that in it as well for other things like column references? probably, right?
				$name = $table['alias'] ?? $table_name;
				$schema = QueryContext::getSchema($database, $table_name);
				if ($schema === null && QueryContext::$strictSchemaMode) {
					throw new SQLFakeRuntimeException(
						"Table $table_name not found in schema and strict mode is enabled",
					);
				}

				$table_data = $conn->getServer()->getTableData($database, $table_name) ?:
					tuple(dict[], dict[], keyset[]);

				$res = $table_data[0];
				$new_index_refs = $table_data[1];
				$new_dirty_pks = $table_data[2] ?? keyset[];

				if (C\count($this->tables) > 1) {
					$new_index_refs = Dict\map_keys($new_index_refs, $k ==> $name.'.'.$k);
				}

				if ($schema is nonnull) {
					if (C\count($this->tables) > 1) {
						$new_indexes = Vec\map(
							$schema->indexes,
							$index ==> new Index(
								$name.'.'.$index->name,
								'INDEX',
								Keyset\map($index->fields, $k ==> $name.'.'.$k),
							),
						);
					} else {
						$new_indexes = $schema->indexes;
					}

					if ($schema->vitess_sharding) {
						$prefix = C\count($this->tables) > 1 ? $name.'.' : '';
						$new_indexes[] = new Index(
							$prefix.$schema->vitess_sharding->keyspace,
							'INDEX',
							keyset[$prefix.$schema->vitess_sharding->sharding_key],
							true,
						);
					}

					$new_columns = dict[];

					foreach ($schema->fields as $field) {
						if (C\count($this->tables) > 1) {
							$new_columns[$name.'.'.$field->name] = $field;
						} else {
							$new_columns[$field->name] = $field;
						}
					}

					$columns = Dict\merge($columns, $new_columns);
				}

				$index_refs = Dict\merge($index_refs, $new_index_refs);
				$dirty_pks = Keyset\union($dirty_pks, $new_dirty_pks);
			}

			$new_dataset = dict[];
			if ($schema is nonnull && QueryContext::$strictSchemaMode) {
				foreach ($res as $key => $row) {
					$row as dict<_, _>;
					$m = dict[];
					foreach ($row as $field => $val) {
						$m["{$name}.{$field}"] = $val;
					}
					$new_dataset[$key] = $m;
				}
			} else if ($schema is nonnull) {
				// if schema is set, order the fields in the right order on each row
				$ordered_fields = keyset[];
				foreach ($schema->fields as $field) {
					$ordered_fields[] = $field->name;
				}

				foreach ($res as $key => $row) {
					invariant($row is dict<_, _>, 'each item in evaluated result of SubqueryExpression must be row');

					$m = dict[];
					foreach ($ordered_fields as $field) {
						if (!C\contains_key($row, $field)) {
							continue;
						}
						$m["{$name}.{$field}"] = $row[$field];
					}
					$new_dataset[$key] = $m;
				}
			} else {
				foreach ($res as $key => $row) {
					invariant($row is dict<_, _>, 'each item in evaluated result of SubqueryExpression must be row');

					$m = dict[];
					foreach ($row as $column => $val) {
						$m["{$name}.{$column}"] = $val;
					}
					$new_dataset[$key] = $m;
				}
			}

			if ($data || !$is_first_table) {
				// do the join here. based on join type, pass in $data and $res to filter. and aliases
				list($data, $index_refs, $dirty_pks) = JoinProcessor::process(
					$conn,
					tuple($data, $index_refs, keyset[]),
					tuple($new_dataset, $new_index_refs, $new_dirty_pks),
					$name,
					$table['join_type'],
					$table['join_operator'] ?? null,
					$table['join_expression'] ?? null,
					$schema,
					$indexes,
					$new_indexes,
				);
			} else {
				$data = $new_dataset;
			}

			$indexes = Vec\concat($indexes, $new_indexes);

			if ($is_first_table) {
				Metrics::trackQuery(QueryType::SELECT, $conn->getServer()->name, $name, $sql);
				$is_first_table = false;
			}
		}

		return tuple($data, $index_refs, $dirty_pks, $indexes, $columns);
	}
}
