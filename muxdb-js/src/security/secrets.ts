export class SecretsLoader {
  private providers: Map<string, (key: string) => string> = new Map();

  constructor() {
    this.registerProvider('env', (key) => process.env[key] || '');
  }

  public registerProvider(scheme: string, provider: (key: string) => string): void {
    this.providers.set(scheme.toLowerCase(), provider);
  }

  public resolve(secretRef: string): string {
    if (!secretRef) return '';
    if (!secretRef.includes(':')) return secretRef;

    const parts = secretRef.split(':');
    const scheme = (parts[0] as string).toLowerCase();
    const reference = parts.slice(1).join(':');


    const provider = this.providers.get(scheme);
    if (provider) {
      return provider(reference);
    }

    if (scheme === 'vault') {
      return `mock-vault-resolved-${reference.replace(/\//g, '-').replace(/#/g, '-')}`;
    }
    if (scheme === 'aws') {
      return `mock-aws-resolved-${reference.replace(/#/g, '-')}`;
    }

    return secretRef;
  }
}
