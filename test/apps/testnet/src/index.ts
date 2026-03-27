import std_listener from './util/std_listener';
import WebsocketServer from './server/websocket';
import ServerContract from './server/server_contract';

std_listener();

const generatorsPath = process.env.GENERATORS_PATH || './generators';

const servers: [ServerContract] = [
  new WebsocketServer(
    parseInt(process.env.FLEZHA_TESTNET_WS_PORT || '4500'),
    generatorsPath
  ),
];

servers.forEach((server) => server.start());
