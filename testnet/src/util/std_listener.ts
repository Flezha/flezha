export default function() {
  process.stdin.resume();
  process.stdin.on('end', () => {
    console.log('Stdin closed, shutting down server.');
    process.exit(0);
  });
};


