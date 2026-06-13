import * as crypto from 'crypto';

function base64urlEncode(str: string | Buffer): string {
  const buf = Buffer.isBuffer(str) ? str : Buffer.from(str);
  return buf.toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function base64urlDecode(str: string): Buffer {
  let base64 = str.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4) {
    base64 += '=';
  }
  return Buffer.from(base64, 'base64');
}

export function validateApiKey(key: string, expectedKey: string): boolean {
  /** Securely compare API keys using constant-time comparison to prevent timing attacks. */
  if (!key || !expectedKey) return false;
  const keyBuf = Buffer.from(key);
  const expBuf = Buffer.from(expectedKey);
  if (keyBuf.length !== expBuf.length) {
    return false;
  }
  return crypto.timingSafeEqual(keyBuf, expBuf);
}

export function generateToken(payload: Record<string, any>, secret: string, expiresInSeconds: number = 3600): string {
  /** Generate a JWT token using HMAC-SHA256. */
  const header = { alg: 'HS256', typ: 'JWT' };
  const payloadCopy = { ...payload, exp: Math.floor(Date.now() / 1000) + expiresInSeconds };
  
  const headerEncoded = base64urlEncode(JSON.stringify(header));
  const payloadEncoded = base64urlEncode(JSON.stringify(payloadCopy));
  
  const signingInput = `${headerEncoded}.${payloadEncoded}`;
  const hmac = crypto.createHmac('sha256', secret);
  hmac.update(signingInput);
  const signatureEncoded = base64urlEncode(hmac.digest());
  
  return `${signingInput}.${signatureEncoded}`;
}

export function verifyToken(token: string, secret: string): Record<string, any> | null {
  /** Verify a JWT token. Returns the decoded payload if valid, otherwise null. */
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    
    const headerEncoded = parts[0] as string;
    const payloadEncoded = parts[1] as string;
    const signatureEncoded = parts[2] as string;
    
    const signingInput = `${headerEncoded}.${payloadEncoded}`;
    
    const hmac = crypto.createHmac('sha256', secret);
    hmac.update(signingInput);
    const expectedSignatureEncoded = base64urlEncode(hmac.digest());
    
    const expectedBuf = Buffer.from(expectedSignatureEncoded);
    const actualBuf = Buffer.from(signatureEncoded);
    
    if (expectedBuf.length !== actualBuf.length || !crypto.timingSafeEqual(expectedBuf, actualBuf)) {
      return null;
    }
    
    const payloadJson = base64urlDecode(payloadEncoded).toString('utf8');

    const payload = JSON.parse(payloadJson);
    
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
      return null;
    }
    
    return payload;
  } catch {
    return null;
  }
}
