import './ws_server';

// Listen to stdin. If it closes, shut down the server.
process.stdin.resume();
process.stdin.on('end', () => {
  console.log('Stdin closed, shutting down server.');
  process.exit(0);
});