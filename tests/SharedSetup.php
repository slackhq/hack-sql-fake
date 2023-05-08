<?hh // strict

namespace Slack\SQLFake;

final class SharedSetup {
	public static async function initAsync(): Awaitable<AsyncMysqlConnection> {
		$schema = get_test_schema();
		init($schema, true);

		$pool = new AsyncMysqlConnectionPool(darray[]);
		$conn = await $pool->connect('example', 1, 'db2', '', '');

		// populate database state
		$database = dict[
			'table3' => vec[
				dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
				dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
				dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
				dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
				dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
			],
			'table4' => vec[
				dict['id' => 1000, 'group_id' => 12345, 'description' => 'desc1'],
				dict['id' => 1001, 'group_id' => 12345, 'description' => 'desc2'],
				dict['id' => 1002, 'group_id' => 12345, 'description' => 'desc3'],
				dict['id' => 1003, 'group_id' => 7, 'description' => 'desc1'],
				dict['id' => 1004, 'group_id' => 7, 'description' => 'desc2'],
			],
			'table5' => vec[
				dict['id' => 1000, 'test_type' => 0x0, 'description' => 'desc0'],
				dict['id' => 1001, 'test_type' => 0x1, 'description' => 'desc1'],
				dict['id' => 1002, 'test_type' => 0x1, 'description' => 'desc2'],
				dict['id' => 1003, 'test_type' => 0x2, 'description' => 'desc3'],
				dict['id' => 1004, 'test_type' => 0x1, 'description' => 'desc4'],
			],
			'association_table' => vec[
				dict['table_3_id' => 1, 'table_4_id' => 1000, 'group_id' => 12345, 'description' => 'association 1'],
				dict['table_3_id' => 1, 'table_4_id' => 1001, 'group_id' => 12345, 'description' => 'association 2'],
				dict['table_3_id' => 2, 'table_4_id' => 1000, 'group_id' => 12345, 'description' => 'association 3'],
				dict['table_3_id' => 3, 'table_4_id' => 1003, 'group_id' => 0, 'description' => 'association 4'],
			],
			'table6' => vec[
				dict['id' => 1000, 'position' => '5'],
				dict['id' => 1001, 'position' => '125'],
				dict['id' => 1002, 'position' => '75'],
				dict['id' => 1003, 'position' => '625'],
				dict['id' => 1004, 'position' => '25'],
			],
		];

		$conn->getServer()->databases['db2'] = $database;

		snapshot('setup');
		return $conn;
	}

	public static async function initVitessAsync(): Awaitable<AsyncMysqlConnection> {
		$schema = get_vitess_test_schema();
		init($schema, true);

		$pool = new AsyncMysqlConnectionPool(darray[]);
		$vitess_conn = await $pool->connect('example2', 2, 'vitess', '', '');

		$vitess_dbs = dict[
			'vt_table1' => vec[
				dict['id' => 1, 'name' => 'Pallettown Chickenstrips'],
				dict['id' => 2, 'name' => 'Brewery Cuttlefish'],
				dict['id' => 3, 'name' => 'Blasphemy Chowderpants'],
				dict['id' => 4, 'name' => 'Benjamin Ampersand'],
			],
			'vt_table2' => vec[
				dict['id' => 11, 'vt_table1_id' => 1, 'description' => 'no'],
				dict['id' => 12, 'vt_table1_id' => 2, 'description' => 'no'],
				dict['id' => 13, 'vt_table1_id' => 3, 'description' => 'no'],
				dict['id' => 14, 'vt_table1_id' => 4, 'description' => 'no'],
			],
		];

		$vitess_conn->getServer()->databases['vitess'] = $vitess_dbs;
		$vitess_conn->getServer()->setConfig(shape(
			'mysql_version' => '5.7',
			'is_vitess' => true,
			'strict_sql_mode' => false,
			'strict_schema_mode' => false,
			'inherit_schema_from' => 'vitess',
		));

		snapshot('vitess_setup');
		return $vitess_conn;
	}

}

<<__Memoize>>
function get_test_schema(): dict<string, dict<string, TableSchema>> {
	return dict[
		'db1' => dict[
			'table1' => new TableSchema(
				'table1',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('name', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('name_uniq', 'UNIQUE', keyset['name']),
				],
			),
			'table2' => new TableSchema(
				'table2',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('table_1_id', DataType::BIGINT, 20, false, 'int'),
					new Column('description', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('table_1_id', 'INDEX', keyset['table_1_id']),
				],
			),
			'table_with_json' => new TableSchema(
				'table_with_json',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('data', DataType::JSON, 255, true, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
				],
			),
			'table_with_more_fields' => new TableSchema(
				'table_with_more_fields',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('name', DataType::VARCHAR, 255, false, 'string'),
					new Column('nullable_unique', DataType::VARCHAR, 255, true, 'string'),
					new Column('nullable_default', DataType::INT, 20, true, 'int', null, '1'),
					new Column('not_null_default', DataType::INT, 20, false, 'int', null, '2'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id', 'name']),
					new Index('nullable_unique', 'UNIQUE', keyset['nullable_unique']),
				],
			),
		],
		'db2' => dict[
			'table3' => new TableSchema(
				'table3',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('group_id', DataType::BIGINT, 20, false, 'int'),
					new Column('name', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('name_uniq', 'UNIQUE', keyset['name']),
					new Index('group_id', 'INDEX', keyset['group_id']),
				],
			),
			'table4' => new TableSchema(
				'table4',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('group_id', DataType::BIGINT, 20, false, 'int'),
					new Column('description', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('group_id', 'INDEX', keyset['group_id']),
				],
			),
			'table5' => new TableSchema(
				'table5',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('test_type', DataType::INT, 16, false, 'int'),
					new Column('description', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('test_type', 'INDEX', keyset['test_type']),
				],
			),
			'table6' => new TableSchema(
				'table6',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('position', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
				],
			),
			'association_table' => new TableSchema(
				'association_table',
				vec[
					new Column('table_3_id', DataType::BIGINT, 20, false, 'int'),
					new Column('table_4_id', DataType::BIGINT, 20, false, 'int'),
					new Column('description', DataType::VARCHAR, 255, false, 'string'),
					new Column('group_id', DataType::BIGINT, 20, false, 'int'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['table_3_id', 'table_4_id']),
					new Index('table_4_id', 'INDEX', keyset['table_4_id']),
				],
			),
		],

	];
}

function get_vitess_test_schema(): dict<string, dict<string, TableSchema>> {
	return dict[
		'vitess' => dict[
			'vt_table1' => new TableSchema(
				'vt_table1',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('name', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('name_uniq', 'UNIQUE', keyset['name']),
				],
				new VitessSharding('test_keyspace_one', 'id'),
			),
			'vt_table2' => new TableSchema(
				'vt_table2',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('vt_table1_id', DataType::BIGINT, 20, false, 'int'),
					new Column('description', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('table_1_id', 'INDEX', keyset['vt_table1_id']),
				],
				new VitessSharding('test_keyspace_one', 'vt_table1_id'),

			),
			'vt_table_with_more_fields' => new TableSchema(
				'vt_table_with_more_fields',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int'),
					new Column('name', DataType::VARCHAR, 255, false, 'string'),
					new Column('nullable_unique', DataType::VARCHAR, 255, true, 'string'),
					new Column('nullable_default', DataType::INT, 20, true, 'int', null, '1'),
					new Column('not_null_default', DataType::INT, 20, false, 'int', null, '2'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id', 'name']),
					new Index('nullable_unique', 'UNIQUE', keyset['nullable_unique']),
				],
				new VitessSharding('test_keyspace_two', 'name'),

			),
		],
	];
}
