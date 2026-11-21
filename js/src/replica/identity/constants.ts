export const GRAPHQL_NAME = /^[_A-Za-z][_0-9A-Za-z]*$/;
export const MAX_VARIABLE_CODEC_DEPTH = 64;
export const FILTER_OPERATORS = new Set([
	'_eq',
	'_neq',
	'_gt',
	'_gte',
	'_lt',
	'_lte',
	'_in',
	'_nin',
	'_is_null',
	'_like',
	'_ilike',
	'_icontains',
	'_contains',
	'_contained_in',
	'_has_key'
]);
