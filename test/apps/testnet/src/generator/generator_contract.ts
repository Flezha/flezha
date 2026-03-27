/**
 * Interface that all testnet data generators must implement.
 *
 * Generator modules are loaded from the directory specified by the
 * GENERATORS_PATH environment variable. Each module must export an object
 * conforming to this interface.
 *
 * The testnet calls `generate(config)` on each tick to produce a message
 * string that is sent over the WebSocket connection to the subscriber.
 */
export interface Generator {
  /** Unique name identifying this generator type (e.g. "trade", "order_book"). */
  name: string;

  /**
   * Produce a single message string to send to the subscriber.
   * Called on each interval tick. The config object is passed from the
   * subscriber's generator entry and allows per-subscription customization.
   */
  generate(config: any): string;

  /** Default configuration used when the subscriber provides no config. */
  defaultConfig: any;
}

/**
 * Subscription entry for a single generator within a subscribe event.
 */
export interface GeneratorSubscription {
  /** Generator type name — must match a loaded generator's `name`. */
  type: string;

  /** Interval in milliseconds between generated messages. */
  interval: number;

  /** Optional config passed to `generator.generate()`. */
  config?: any;
}
