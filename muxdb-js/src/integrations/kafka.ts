import { logger } from '../observability';

export class MuxKafkaProducer {
  public sentMessages: Array<Record<string, any>> = [];

  constructor(
    public readonly kafkaConfig: Record<string, any>,
    private readonly router: any
  ) {}

  public async produce(
    topic: string,
    key: string,
    value: string,
    opts: { numPartitions?: number } = {}
  ): Promise<void> {
    /** Route updates to Kafka topics partitioned by MuxDB Shard ID. */
    const shard = this.router.routeKey(key);
    const numPartitions = opts.numPartitions || 10;
    
    // Simple string hash function
    let hash = 0;
    for (let i = 0; i < shard.id.length; i++) {
      hash = (hash << 5) - hash + shard.id.charCodeAt(i);
      hash |= 0;
    }
    const partition = Math.abs(hash) % numPartitions;

    const payload = {
      topic,
      key,
      value,
      partition,
      targetShardId: shard.id,
    };
    this.sentMessages.push(payload);
    
    logger.info(payload, 'kafka.mock_produce');
  }
}

export class MuxKafkaConsumer {
  private handlers: Map<string, (msg: any) => void> = new Map();

  constructor(public readonly kafkaConfig: Record<string, any>) {}

  public subscribe(topics: string[]): void {
    /** Subscribe to topics. */
    logger.info({ topics }, 'kafka.mock_subscribe');
  }

  public registerHandler(topic: string, handler: (msg: any) => void): void {
    /** Register callback for a topic. */
    this.handlers.set(topic, handler);
  }

  public async poll(timeoutSeconds: number = 1): Promise<any> {
    /** Poll CDC message stream. */
    await new Promise((resolve) => setTimeout(resolve, timeoutSeconds * 1000));
    return null;
  }
}
