import * as tls from 'tls';
import * as fs from 'fs';

export function createClientSSLOptions(
  caCertPath?: string,
  certChainPath?: string,
  privateKeyPath?: string
): tls.ConnectionOptions {
  /** Create standard Node.js ConnectionOptions for secure clients. */
  const options: tls.ConnectionOptions = {
    rejectUnauthorized: true,
  };
  
  if (caCertPath) {
    options.ca = fs.readFileSync(caCertPath);
  }
  
  if (certChainPath && privateKeyPath) {
    options.cert = fs.readFileSync(certChainPath);
    options.key = fs.readFileSync(privateKeyPath);
  }
  
  return options;
}

export function createServerSSLOptions(
  certChainPath: string,
  privateKeyPath: string,
  caCertPath?: string,
  requireClientAuth: boolean = false
): tls.SecureContextOptions & { requestCert?: boolean; rejectUnauthorized?: boolean } {
  /** Create SecureContextOptions for a Node.js server (e.g. gRPC or HTTPS). */
  const options: tls.SecureContextOptions & { requestCert?: boolean; rejectUnauthorized?: boolean } = {
    cert: fs.readFileSync(certChainPath),
    key: fs.readFileSync(privateKeyPath),
  };
  
  if (caCertPath) {
    options.ca = fs.readFileSync(caCertPath);
  }
  
  if (requireClientAuth) {
    if (!caCertPath) {
      throw new Error('caCertPath is required when requireClientAuth is true');
    }
    options.requestCert = true;
    options.rejectUnauthorized = true;
  }
  
  return options;
}

