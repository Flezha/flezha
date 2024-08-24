use std::io::{self, BufRead};

fn main() {
    // Create a buffered reader for stdin
    let stdin = io::stdin();
    let reader = stdin.lock();

    // Read lines from stdin in a blocking way
    for line in reader.lines() {
        match line {
            Ok(command_data) => {
                
            }
            Err(e) => {
                eprintln!("Failed to read line: {}", e);
            }
        }
    }
}