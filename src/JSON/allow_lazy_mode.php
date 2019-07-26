<?hh // strict

namespace Slack\SQLFake\JSON;

/**
 * In lazy mode I'll presume that the value is valid JSON.
 * This will allow me to skip the potentially expensive json_decode step.
 * This will be better implementated later.
 */
function allow_lazy_mode(): bool {
	return true;
}
