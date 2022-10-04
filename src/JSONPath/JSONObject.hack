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
 * Modified heavily by maintainers of hack-sql-fake to hacklang & to only support MySQL's JSON path.
 */

namespace Slack\SQLFake\JSONPath;

use namespace HH\Lib\{C, Regex, Str, Vec};

type ExplodedPathType = vec<arraykey>;

type ObjectAtPath = shape(
    'object' => mixed,
    'path' => ExplodedPathType,
);

type MatchedObjectsResult = shape(
    'matched' => vec<ObjectAtPath>,
    'divergingPath' => bool,
);

type GetOptions = shape(
    'unwrap' => bool, // Directly return the object found (no outer vec[]) for non-branching path. default: false
);

class WrappedResult<T> {
    public function __construct(public T $value) {}
}

/**
 * This is a [JSONPath](http://goessner.net/articles/JsonPath/) like implementation for PHP.
 * It only aims to support JSONPath as implemented/supported by MySQL.
 *
 * Usage
 * =====
 *       // $json can be a string containing json, a `vec`, a `dict`, a PHP object or null.
 *       // If $json is null (or not present) the JSONObject will be empty.
 *       $jsonObject = new JSONObject();
 *       // or
 *       $jsonObject = new JSONObject($json);
 *
 *       // get
 *       $obj->get($jsonPath);
 *
 *       // replace
 *       $obj->replace($jsonPath, $value);
 *
 *       // get the json representation
 *       $str = $obj->getJSON();
 *       echo $str;
 *
 *       // get the hacklang representation
 *       $obj->getValue();
 *
 *
 * Get options
 * --------
 *
 * GetOptions can be passed optionally to $obj->get() to control behavior.
 *
 *      shape(unwrap => false) // Always wrap results in vec[]
 *      shape(unwrap => true)  // Directly return the found object if path does not contain wildcards (non-branching)
 *
 * What `unwrap` does is to determine if the given JsonPath branches at some point, if it does it behaves as usual;
 * otherwise, it will directly return the value pointed to by the given path (not a vec containing a single value).
 *
 *      $json = dict[
 *          "a" => dict[
 *              "b" => 3,
 *              "c" => 4
 *          ]
 *      ];
 *      $obj = new JSONObject($json);
 *      $obj->get('$.a.b', shape('unwrap' => true)); // Returns int(3)
 *      $obj->get('$.a.*', shape('unwrap' => true)); // Returns vec[int(3), int(4)]
 *
 */
class JSONObject {
    // Tokens
    const TOK_ROOT = '$';
    const TOK_SELECTOR_BEGIN = '[';
    const TOK_SELECTOR_END = ']';
    const TOK_ALL = '*';
    const TOK_CHILD_ACCESS_BEGIN = '.';
    const TOK_DOUBLE_ASTERISK = '**';

    private mixed $jsonObject; // This is any valid type in JSON (vec, dict, num, string, null)

    /**
     * Class constructor.
     * If $json is null the json object contained will be initialized empty.
     */
    public function __construct(mixed $json = null) {
        if ($json is string) {
            $this->jsonObject = \json_decode($json, true, 512, \JSON_FB_HACK_ARRAYS);
            if ($json !== 'null' && $this->jsonObject === null) {
                throw new InvalidJSONException('string does not contain a valid JSON object.');
            }
        } else if ($json is vec<_> || $json is dict<_, _> || \is_object($json)) {
            // We encode & decode here to make sure we only have nested dicts/vecs & dict keys are string only
            $this->jsonObject = \json_decode(\json_encode($json), true, 512, \JSON_FB_HACK_ARRAYS);
        } else {
            throw new InvalidJSONException('value does not encode a JSON object.');
        }
    }

    /**
     * Returns the value of the JSON object
     */
    public function getValue(): mixed {
        return $this->jsonObject;
    }

    /**
    * Returns the JSON representation of the JSON object
    */
    public function getJSON(): string {
        return \json_encode($this->jsonObject);
    }

    /**
     * Returns an vec containing objects that match the JsonPath.
     *
     * If smartGet was set to true when creating the instance and
     * the JsonPath given does not branch, it will return the value
     * instead of a vec of size 1.
     */
    public function get(string $jsonPath, GetOptions $opts = shape('unwrap' => false)): ?WrappedResult<mixed> {
        $unwrap = $opts['unwrap'] ?? false;

        $jsonObject = $this->jsonObject;
        $result = self::getMatching($jsonObject, $jsonPath);

        $matched = $result['matched'];
        $divergingPath = $result['divergingPath'];
        if (C\is_empty($matched)) {
            return null;
        }

        if ($unwrap && !$divergingPath) {
            return new WrappedResult($matched[0]['object']);
        }

        return new WrappedResult(Vec\map($matched, $o ==> $o['object']));
    }

    /**
     * Replaces the element that results from the $jsonPath query
     * to $value. This method disallows divering (wildcard) paths.
     * It is a no-op if the given path doesn't already exist.
     *
     * This method returns a new JsonObject with the new value.
     */
    public function replace(string $jsonPath, mixed $value): WrappedResult<JSONObject> {
        $result = self::getMatching($this->jsonObject, $jsonPath);
        if ($result['divergingPath']) {
            throw new DivergentJSONPathSetException('Cannot set a value using a wildcard JSON path');
        }

        $out = self::setPathsToValue(
            shape('object' => $this->jsonObject, 'path' => vec[self::TOK_ROOT]),
            Vec\map($result['matched'], $m ==> $m['path']),
            $value,
        );

        return new WrappedResult(new JSONObject(\json_encode($out)));
    }

    /**
     * Returns the keys of the object located by the $jsonPath query.
     * This method disallows divering (wildcard) paths.
     *
     * This method returns null (if object not located) or a vec[] of the keys.
     */
    public function keys(string $jsonPath = '$'): ?WrappedResult<vec<string>> {
        $result = self::getMatching($this->jsonObject, $jsonPath);
        if ($result['divergingPath']) {
            throw new DivergentJSONPathSetException('Cannot get keys using a wildcard JSON path');
        }

        $matches = $result['matched'];
        if (C\is_empty($matches)) {
            return null;
        }

        $matched = $matches[0]['object'];
        if (!($matched is dict<_, _>)) {
            return null;
        }

        $keys = vec[];
        foreach ($matched as $k => $_v) {
            invariant($k is string, 'dict cannot have non string key in JSON');
            $keys[] = $k;
        }

        return new WrappedResult($keys);
    }

    /**
     * Returns the length of the value located by the $jsonPath query.
     * This method disallows divering (wildcard) paths.
     *
     * This method returns null (if object not located) or a vec[] of the keys.
     */
    public function length(string $jsonPath = '$'): ?WrappedResult<int> {
        $result = self::getMatching($this->jsonObject, $jsonPath);
        if ($result['divergingPath']) {
            throw new DivergentJSONPathSetException('Cannot get length using a wildcard JSON path');
        }

        $matches = $result['matched'];
        if (C\is_empty($matches)) {
            return null;
        }

        $matched = $matches[0]['object'];
        if ($matched is dict<_, _> || $matched is vec<_>) {
            return new WrappedResult(C\count($matched));
        }

        return new WrappedResult(1);
    }

    /**
     * Returns the maximum depth of the value.
     */
    public function depth(): WrappedResult<int> {
        $depth = 0;

        $objects = vec[$this->jsonObject];
        while (!C\is_empty($objects)) {
            $children = vec[];

            foreach ($objects as $object) {
                if ($object is dict<_, _> || $object is vec<_>) {
                    foreach ($object as $child) {
                        $children[] = $child;
                    }
                }
            }

            // Each time we enter this while loop, it means we just processed a new level
            $depth += 1;
            $objects = $children;
        }

        return new WrappedResult($depth);
    }

    private static function pathMatched(vec<ExplodedPathType> $paths, ExplodedPathType $path): bool {
        return C\contains($paths, $path);
    }

    private static function setPathsToValue(ObjectAtPath $object, vec<ExplodedPathType> $paths, mixed $value): mixed {
        $jsonObject = $object['object'];
        $path = $object['path'];

        // Found something to be replaced!
        if (self::pathMatched($paths, $path)) {
            return $value;
        }

        if ($jsonObject is vec<_> || $jsonObject is dict<_, _>) {
            $out = $jsonObject is vec<_> ? vec[] : dict[];
            foreach ($jsonObject as $key => $original_value) {
                $childPath = Vec\concat($path, vec[$key]);
                $newValue = self::setPathsToValue(
                    shape('object' => $original_value, 'path' => $childPath),
                    $paths,
                    $value,
                );

                if ($out is vec<_>) {
                    $out[] = $newValue;
                } else if ($out is dict<_, _>) {
                    $out[$key] = $newValue;
                }
            }

            return $out;
        }

        return $jsonObject;
    }

    private static function matchArrayIndex(string $jsonPath): ?shape('index' => string, 'rest' => string) {
        if (Str\is_empty($jsonPath) || $jsonPath[0] !== self::TOK_SELECTOR_BEGIN) {
            return null;
        }

        $array_idx_regex = re"/^\[(?<index>\*|\d+)\](?<rest>.*)$/";
        $matched = Regex\first_match($jsonPath, $array_idx_regex);
        if ($matched) {
            return shape(
                'index' => $matched['index'],
                'rest' => $matched['rest'],
            );
        }

        return null;
    }

    private static function matchChildAccess(string $jsonPath): ?shape('child' => string, 'rest' => string) {
        if (Str\is_empty($jsonPath) || $jsonPath[0] != self::TOK_CHILD_ACCESS_BEGIN) {
            return null;
        }

        $child_name_regex = re"/^\.(?<child>(?<quote0>\"?)[[:alpha:]_$][a-zA-Z0-9_\-\$]*(?<quote1>\\2)|\*)(?<rest>.*)$/";
        $matched = Regex\first_match($jsonPath, $child_name_regex);

        if ($matched) {
            // Remove double quotedness if matched
            if ($matched['quote0'] === '"' && $matched['quote1'] === '"') {
                $child = Str\strip_prefix($matched['child'], '"') |> Str\strip_suffix($$, '"');
            } else {
                $child = $matched['child'];
            }

            return shape(
                'child' => $child,
                'rest' => $matched['rest'],
            );
        }

        return null;
    }

    private static function opChildName(ObjectAtPath $objectAtPath, string $childName): MatchedObjectsResult {
        $out = vec[];

        $jsonObject = $objectAtPath['object'];
        $path = $objectAtPath['path'];

        $diverged = false;

        if ($jsonObject is KeyedContainer<_, _>) {
            if ($childName === self::TOK_ALL) {
                $diverged = true;

                foreach ($jsonObject as $key => $item) {
                    // We ignore int indices (varray) here
                    if ($key is int) {
                        continue;
                    }

                    $out[] = shape(
                        'object' => $item,
                        'path' => Vec\concat($path, vec[$key]),
                    );
                }
            } else if (C\contains_key($jsonObject, $childName)) {
                $out[] = shape(
                    'object' => $jsonObject[$childName],
                    'path' => Vec\concat($path, vec[$childName]),
                );
            }
        }

        return shape('matched' => $out, 'divergingPath' => $diverged);
    }

    private static function opChildSelector(ObjectAtPath $objectAtPath, string $contents): MatchedObjectsResult {
        $jsonObject = $objectAtPath['object'];
        $path = $objectAtPath['path'];

        if ($jsonObject is KeyedContainer<_, _>) {
            if ($contents === self::TOK_ALL) {
                $out = vec[];
                foreach ($jsonObject as $key => $item) {
                    $out[] = shape(
                        'object' => $item,
                        'path' => Vec\concat($path, vec[$key]),
                    );
                }
                return shape('matched' => $out, 'divergingPath' => true);
            }

            $index = Str\to_int($contents);
            if ($index is nonnull) {
                if (C\contains_key($jsonObject, $index)) {
                    return shape(
                        'matched' => vec[shape(
                            'object' => $jsonObject[$index],
                            'path' => Vec\concat($path, vec[$index]),
                        )],
                        'divergingPath' => false,
                    );
                }

                return shape('matched' => vec[], 'divergingPath' => false);
            }

            throw new InvalidJSONPathException($contents);
        }

        return shape('matched' => vec[], 'divergingPath' => false);
    }

    private static function matchRecursiveSelector(
        string $jsonPath,
    ): ?shape(?'index' => string, ?'child' => string, 'rest' => string) {
        if (!Str\starts_with($jsonPath, self::TOK_DOUBLE_ASTERISK)) {
            return null;
        }

        $jsonPath = Str\strip_prefix($jsonPath, self::TOK_DOUBLE_ASTERISK);

        $matchedArrayIndex = self::matchArrayIndex($jsonPath);
        if ($matchedArrayIndex) {
            return shape('index' => $matchedArrayIndex['index'], 'rest' => $matchedArrayIndex['rest']);
        }

        $matchedChildAccess = self::matchChildAccess($jsonPath);
        if ($matchedChildAccess) {
            return shape('child' => $matchedChildAccess['child'], 'rest' => $matchedChildAccess['rest']);
        }

        return null;
    }

    private static function opRecursiveSelector(
        ObjectAtPath $objectAtPath,
        shape(?'index' => string, ?'child' => string, 'rest' => string) $matched,
    ): MatchedObjectsResult {
        $out = vec[];

        $jsonObject = $objectAtPath['object'];
        $path = $objectAtPath['path'];

        $childName = $matched['child'] ?? null;
        if ($childName is nonnull) {
            $ret = self::opChildName($objectAtPath, $childName);
            $out = Vec\concat($out, $ret['matched']);

            if ($jsonObject is KeyedContainer<_, _>) {
                foreach ($jsonObject as $key => $value) {
                    $ret = self::opRecursiveSelector(
                        shape('object' => $value, 'path' => Vec\concat($path, vec[$key])),
                        $matched,
                    );
                    $out = Vec\concat($out, $ret['matched']);
                }
            }
        }

        $index = $matched['index'] ?? null;
        if ($index is nonnull) {
            $ret = self::opChildSelector($objectAtPath, $index);
            $out = Vec\concat($out, $ret['matched']);

            if ($jsonObject is KeyedContainer<_, _>) {
                foreach ($jsonObject as $key => $value) {
                    $ret = self::opRecursiveSelector(
                        shape('object' => $value, 'path' => Vec\concat($path, vec[$key])),
                        $matched,
                    );
                    $out = Vec\concat($out, $ret['matched']);
                }
            }
        }

        return shape('matched' => $out, 'divergingPath' => true);
    }

    static private function getMatching(mixed $jsonObject, string $jsonPath): MatchedObjectsResult {
        if (!Str\starts_with($jsonPath, self::TOK_ROOT)) {
            throw new InvalidJSONPathException($jsonPath);
        }

        $jsonPath = Str\strip_prefix($jsonPath, self::TOK_ROOT);
        $selection = vec[shape('object' => $jsonObject, 'path' => vec[self::TOK_ROOT])];
        $divergingPath = false;
        while (!Str\is_empty($jsonPath) && !C\is_empty($selection)) {
            $newSelection = vec[];

            $matchedChildAccess = self::matchChildAccess($jsonPath);
            if ($matchedChildAccess is nonnull) {
                foreach ($selection as $jsonObject) {
                    $ret = self::opChildName($jsonObject, $matchedChildAccess['child']);
                    $divergingPath = $divergingPath || $ret['divergingPath'];
                    $newSelection = Vec\concat($newSelection, $ret['matched']);
                }

                if (C\is_empty($newSelection)) {
                    $selection = vec[];
                    break;
                } else {
                    $jsonPath = $matchedChildAccess['rest'];
                }

                $selection = $newSelection;
                continue;
            }

            $matchedArrayIndex = self::matchArrayIndex($jsonPath);
            if ($matchedArrayIndex) {
                $index = $matchedArrayIndex['index'];
                foreach ($selection as $jsonObject) {
                    $ret = self::opChildSelector($jsonObject, $index);
                    $divergingPath = $divergingPath || $ret['divergingPath'];
                    $newSelection = Vec\concat($newSelection, $ret['matched']);
                }

                if (C\is_empty($newSelection)) {
                    $selection = vec[];
                    break;
                } else {
                    $jsonPath = $matchedArrayIndex['rest'];
                }

                $selection = $newSelection;
                continue;
            }

            $matchedRecursive = self::matchRecursiveSelector($jsonPath);
            if ($matchedRecursive) {
                foreach ($selection as $jsonObject) {
                    $ret = self::opRecursiveSelector($jsonObject, $matchedRecursive);
                    $divergingPath = $divergingPath || $ret['divergingPath'];
                    $newSelection = Vec\concat($newSelection, $ret['matched']);
                }

                if (C\is_empty($newSelection)) {
                    $selection = vec[];
                    break;
                } else {
                    $jsonPath = $matchedRecursive['rest'];
                }

                $selection = $newSelection;
                continue;
            }

            throw new InvalidJSONPathException($jsonPath);
        }

        return shape('matched' => $selection, 'divergingPath' => $divergingPath);
    }
}
