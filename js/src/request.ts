/** Isomorphic HTTP GraphQL request helper with injectable URL and authentication. */
import { buildAuthHeaders } from './auth-headers.js';
import {
	DistributedProtocolError,
	parseGraphqlResponseExtensions
} from './protocol.js';
import type {
	GqlAuth,
	GqlError,
	GqlResult,
	GraphqlVariables
} from './types.js';

/** Fetch-compatible transport, including SvelteKit's request-scoped `fetch`. */
export type FetchLike = typeof globalThis.fetch;

export type RequestGraphqlOptions = {
	/** Override the runtime's global fetch implementation. */
	fetch?: FetchLike;
	/** Framework-owned GraphQL request extensions. */
	extensions?: Readonly<Record<string, unknown>>;
	/** Cancel the underlying HTTP request when its authorization generation closes. */
	signal?: AbortSignal;
};

type GraphqlResponseBody<TData> = {
	data?: TData;
	errors?: GqlError[];
	error?: string;
	extensions?: unknown;
};

type ParsedResponseBody<TData> = {
	body: GraphqlResponseBody<TData>;
	rawText?: string;
};

async function readResponseBody<TData>(response: Response): Promise<ParsedResponseBody<TData>> {
	let rawText: string | undefined;
	if (typeof response.text === 'function') {
		try {
			rawText = await response.text();
		} catch {
			// A fetch-compatible test double may only implement json().
		}
	}

	if (rawText !== undefined) {
		try {
			return { body: asResponseBody<TData>(JSON.parse(rawText)) };
		} catch {
			return { body: {}, rawText };
		}
	}

	try {
		return { body: asResponseBody<TData>(await response.json()) };
	} catch {
		return { body: {} };
	}
}

function asResponseBody<TData>(value: unknown): GraphqlResponseBody<TData> {
	return value !== null && typeof value === 'object' && !Array.isArray(value)
		? (value as GraphqlResponseBody<TData>)
		: {};
}

function httpFailureMessage(
	response: Response,
	body: GraphqlResponseBody<unknown>,
	rawText: string | undefined
): string {
	const jsonDetail = typeof body.error === 'string' ? body.error.trim() : '';
	if (jsonDetail) return jsonDetail;

	const statusText = response.statusText?.trim();
	const status = `HTTP ${response.status}${statusText ? ` ${statusText}` : ''}`;
	const textDetail = rawText?.trim() ?? '';
	if (
		!textDetail ||
		/^<(?:!doctype\s+html|html|head|body)(?:\s|>)/i.test(textDetail)
	) {
		return status;
	}

	const maxDetailLength = 500;
	const boundedDetail =
		textDetail.length > maxDetailLength
			? `${textDetail.slice(0, maxDetailLength - 1)}…`
			: textDetail;
	return `${status}: ${boundedDetail}`;
}

/** POST a GraphQL document to `url`. */
export async function requestGraphql<
	TData = Record<string, unknown>,
	TVariables extends GraphqlVariables = GraphqlVariables
>(
	url: string,
	document: string,
	auth: GqlAuth = {},
	variables: TVariables = {} as TVariables,
	options: RequestGraphqlOptions = {}
): Promise<GqlResult<TData>> {
	const fetchImpl = options.fetch ?? globalThis.fetch;
	if (typeof fetchImpl !== 'function') {
		throw new Error(
			'requestGraphql requires a fetch implementation in this runtime; pass { fetch } as the final argument'
		);
	}

	const token = auth.accessToken?.trim() ?? '';
	const response = await fetchImpl(url, {
		method: 'POST',
		headers: buildAuthHeaders(auth),
		body: JSON.stringify({
			query: document,
			variables,
			...(options.extensions === undefined
				? {}
				: { extensions: options.extensions })
		}),
		...(options.signal === undefined ? {} : { signal: options.signal })
	});

	const { body, rawText } = await readResponseBody<TData>(response);
	let extensions;
	try {
		extensions = parseGraphqlResponseExtensions(body.extensions);
	} catch (error) {
		if (error instanceof DistributedProtocolError) {
			return {
				data: undefined,
				errors: [
					...(body.errors ?? []),
					{
						message: error.message,
						extensions: { code: error.code }
					}
				],
				status: response.status
			};
		}
		throw error;
	}

	if (response.status === 401) {
		const detail =
			body.errors?.[0]?.message ??
			body.error ??
			(token
				? 'Bearer rejected (audience/issuer/expiry) — sign out and back in'
				: 'no access token — re-login; check OIDC scopes include the project audience');

		return {
			data: body.data,
			errors: [{ message: detail }],
			...(extensions === undefined ? {} : { extensions }),
			status: response.status
		};
	}

	if (
		response.status >= 400 &&
		response.status < 600 &&
		!body.errors?.length
	) {
		return {
			data: body.data,
			errors: [{ message: httpFailureMessage(response, body, rawText) }],
			...(extensions === undefined ? {} : { extensions }),
			status: response.status
		};
	}

	return {
		data: body.data,
		errors: body.errors,
		...(extensions === undefined ? {} : { extensions }),
		status: response.status
	};
}

export { buildAuthHeaders } from './auth-headers.js';
