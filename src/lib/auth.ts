import { Env } from './types';

const TIMESTAMP_TOLERANCE_SECONDS = 300;
const encoder = new TextEncoder();

function constantTimeEquals(a: string, b: string): boolean {
  let diff = a.length ^ b.length;
  const maxLen = Math.max(a.length, b.length);

  for (let i = 0; i < maxLen; i += 1) {
    const charA = i < a.length ? a.charCodeAt(i) : 0;
    const charB = i < b.length ? b.charCodeAt(i) : 0;
    diff |= charA ^ charB;
  }

  return diff === 0;
}

function parseAffirmSignature(
  header: string
): { timestamp: number; signatures: string[] } | null {
  let timestamp: number | null = null;
  const signatures: string[] = [];

  for (const part of header.split(',')) {
    const [rawKey, rawValue] = part.split('=', 2);
    const key = rawKey?.trim();
    const value = rawValue?.trim();

    if (!key || !value) continue;

    if (key === 't') {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        timestamp = parsed;
      }
      continue;
    }

    if (key === 'v0') {
      signatures.push(value.toLowerCase());
    }
  }

  if (timestamp === null || signatures.length === 0) {
    return null;
  }

  return { timestamp, signatures };
}

async function computeSignature(secret: string, timestamp: number, rawBody: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-512' },
    false,
    ['sign']
  );

  const payload = `${timestamp}.${rawBody}`;
  const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(payload));

  return Array.from(new Uint8Array(signature))
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');
}

export async function isAuthorized(request: Request, env: Env, rawBody: string): Promise<boolean> {
  const header = request.headers.get('x-affirm-signature');
  if (!env.SHARED_SECRET || !header) {
    return false;
  }

  const parsed = parseAffirmSignature(header);
  if (!parsed) {
    return false;
  }

  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - parsed.timestamp) > TIMESTAMP_TOLERANCE_SECONDS) {
    return false;
  }

  const expected = await computeSignature(env.SHARED_SECRET, parsed.timestamp, rawBody);

  return parsed.signatures.some((signature) =>
    constantTimeEquals(signature, expected)
  );
}
