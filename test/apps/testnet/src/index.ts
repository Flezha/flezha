import std_listener from './util/std_listener';
import WebsocketServer from './server/websocket';
import ServerContract from './server/server_contract';
import { ManualDriver, manualInputEnabled } from './server/manual_driver';

const generatorsPath = process.env.GENERATORS_PATH || './generators';

const websocket = new WebsocketServer(
  parseInt(process.env.FLEZHA_TESTNET_WS_PORT || '4500'),
  generatorsPath,
  process.env.FLEZHA_TESTNET_CONFIG
);

const servers: [ServerContract] = [websocket];

const driver = manualInputEnabled(process.env.FLEZHA_TESTNET_MANUAL, process.stdin.isTTY === true)
  ? new ManualDriver(websocket, (line) => console.log(line))
  : null;

std_listener(driver ? (line) => driver.handleLine(line) : undefined);

servers.forEach((server) => server.start());

if (driver) {
  console.log('Manual input enabled. Type /help for the commands.');
}
