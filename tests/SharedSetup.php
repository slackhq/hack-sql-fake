<?hh // strict

namespace Slack\SQLFake;

final class SharedSetup {
	public static async function initAsync(): Awaitable<AsyncMysqlConnection> {
		$schema = get_test_schema();
		init($schema, true);

		$pool = new AsyncMysqlConnectionPool(darray[]);
		$conn = await $pool->connect('example', 1, 'db2', '', '');

		$table3_data = tuple(
			dict[
				1 => dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
				2 => dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
				3 => dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
				4 => dict['id' => 4, 'group_id' => 6, 'name' => 'name4'],
				6 => dict['id' => 6, 'group_id' => 6, 'name' => 'name5'],
			],
			dict[
				'name_uniq' => dict[
					'name1' => 1,
					'name2' => 2,
					'name3' => 3,
					'name4' => 4,
					'name5' => 6,
				],
				'group_id' => dict[
					12345 => keyset[1, 2, 3],
					6 => keyset[4, 6],
				],
			],
		);

		$table4_data = tuple(
			dict[
				1000 => dict['id' => 1000, 'group_id' => 12345, 'description' => 'desc1'],
				1001 => dict['id' => 1001, 'group_id' => 12345, 'description' => 'desc2'],
				1002 => dict['id' => 1002, 'group_id' => 12345, 'description' => 'desc3'],
				1003 => dict['id' => 1003, 'group_id' => 7, 'description' => 'desc1'],
				1004 => dict['id' => 1004, 'group_id' => 7, 'description' => 'desc2'],
			],
			dict[
				'group_id' => dict[
					12345 => keyset[1000, 1001, 1002],
					7 => keyset[1003, 1004],
				],
			],
		);

		$table5_data = tuple(
			dict[
				1000 => dict['id' => 1000, 'test_type' => 0x0, 'description' => 'desc0'],
				1001 => dict['id' => 1001, 'test_type' => 0x1, 'description' => 'desc1'],
				1002 => dict['id' => 1002, 'test_type' => 0x1, 'description' => 'desc2'],
				1003 => dict['id' => 1003, 'test_type' => 0x2, 'description' => 'desc3'],
				1004 => dict['id' => 1004, 'test_type' => 0x1, 'description' => 'desc4'],
			],
			dict[
				'test_type' => dict[
					0x0 => keyset[1000],
					0x1 => keyset[1001, 1002, 1004],
					0x2 => keyset[1003],
				],
			],
		);

		$association_table_data = tuple(
			dict[
				0 => dict[
					'table_3_id' => 1,
					'table_4_id' => 1000,
					'group_id' => 12345,
					'description' => 'association 1',
				],
				1 => dict[
					'table_3_id' => 1,
					'table_4_id' => 1001,
					'group_id' => 12345,
					'description' => 'association 2',
				],
				2 => dict[
					'table_3_id' => 2,
					'table_4_id' => 1000,
					'group_id' => 12345,
					'description' => 'association 3',

				],
				3 => dict[
					'table_3_id' => 3,
					'table_4_id' => 1003,
					'group_id' => 0,
					'description' => 'association 4',

				],
			],
			dict[
				'PRIMARY' => dict[
					1 => dict[
						1000 => 0,
						1001 => 1,
					],
					2 => dict[
						1000 => 2,
					],
					3 => dict[
						1003 => 3,
					],
				],
				'table_4_id' => dict[
					1000 => keyset[0, 2],
					1001 => keyset[1],
					1003 => keyset[3],
				],
			],
		);

		$table6_data = tuple(
			dict[
				1000 => dict['id' => 1000, 'position' => '5'],
				1001 => dict['id' => 1001, 'position' => '125'],
				1002 => dict['id' => 1002, 'position' => '75'],
				1003 => dict['id' => 1003, 'position' => '625'],
				1004 => dict['id' => 1004, 'position' => '25'],
			],
			dict[],
		);

		// populate database state
		$database = dict[
			'table3' => $table3_data,
			'table4' => $table4_data,
			'table5' => $table5_data,
			'association_table' => $association_table_data,
			'table6' => $table6_data,
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
			'vt_table1' => tuple(
				dict[
					1 => dict['id' => 1, 'name' => 'Pallettown Chickenstrips'],
					2 => dict['id' => 2, 'name' => 'Brewery Cuttlefish'],
					3 => dict['id' => 3, 'name' => 'Blasphemy Chowderpants'],
					4 => dict['id' => 4, 'name' => 'Benjamin Ampersand'],
				],
				dict[],
			),
			'vt_table2' => tuple(
				dict[
					11 => dict['id' => 11, 'vt_table1_id' => 1, 'description' => 'no'],
					12 => dict['id' => 12, 'vt_table1_id' => 2, 'description' => 'no'],
					13 => dict['id' => 13, 'vt_table1_id' => 3, 'description' => 'no'],
					14 => dict['id' => 14, 'vt_table1_id' => 4, 'description' => 'no'],
				],
				dict[],
			),
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
