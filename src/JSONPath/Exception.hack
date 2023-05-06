/**
 * Copyright 2018 Alessio Linares
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Based on code from https://github.com/Galbar/JsonPath-PHP
 *
 * Modified by maintainers of hack-sql-fake as needed
 */

namespace Slack\SQLFake\JSONPath;

class JSONException extends \Exception {}
final class InvalidJSONException extends JSONException {}
final class DivergentJSONPathSetException extends JSONException {}
final class InvalidJSONPathException extends JSONException {
	private string $token;

	/**
	 * Class constructor
	 *
	 * @param string $token token related to the JSONPath error
	 *
	 * @return void
	 */
	public function __construct(string $token) {
		$this->token = $token;
		parent::__construct("Error in JSONPath near '".$token."'", 0, null);
	}
}
