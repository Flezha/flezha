import std_listener from './util/std_listener';
import WebsocketServer from './server/websocket';
import ServerContract from './server/server_contract';

std_listener();

const servers: [ServerContract] = [
  new WebsocketServer(parseInt(process.env.AFLECHA_TESTNET_WS_PORT || '4500'))
];

servers.forEach((server) => server.start());
