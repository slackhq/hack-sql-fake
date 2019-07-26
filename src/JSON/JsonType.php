<?hh // strict

namespace Slack\SQLFake\JSON;

enum JsonType: string {
	OBJECT = 'OBJECT';
	ARRAY = 'ARRAY';
	INTEGER = 'INTEGER';
	BOOLEAN = 'BOOLEAN';
	NULL = 'NULL';
	DOUBLE = 'DOUBLE';
	DECIMAL = 'DECIMAL';
	DATETIME = 'DATETIME';
	DATE = 'DATE';
	TIME = 'TIME';
	STRING = 'STRING';
	BLOB = 'BLOB';
	BIT = 'BIT';
	OPAQUE = 'OPAQUE';
}
