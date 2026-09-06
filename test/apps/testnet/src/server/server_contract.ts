import { ConnectionRole } from './connection_paths';

export default interface ServerContract {
  start(): void;
}

/** One connected socket, as a driver sees it. */
export interface ConnectedSocket {
  path: string;
  role: ConnectionRole;
}

/**
 * The sockets a driver can originate traffic on. Implemented by the websocket
 * server; depended on by anything that sends without being asked.
 */
export interface InputSockets {
  /** Send to every socket taking input, and answer how many got it. */
  broadcast(message: string): number;
  /** Every socket currently connected, input and output alike. */
  connected(): ConnectedSocket[];
}
