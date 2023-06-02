mod lib;
use lib::{BookTicker, RollingOHLC, OHLC};
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

fn main() {
    // Open the data file
    let file = File::open("data.txt").unwrap();
    let reader = BufReader::new(file);

    // Create a RollingOHLC instance
    let mut rolling_ohlc = RollingOHLC::new(60);

    // Loop over each line in the file
    for line in reader.lines() {
        // Parse the line as JSON input
        let input: BookTicker = serde_json::from_str(&line.unwrap()).unwrap();
        // Update the RollingOHLC instance with the input
        rolling_ohlc.update(input);
    }

    //Print the OHLC data
    for ohlc in rolling_ohlc.get_ohlc_data() {
        println!(
            "Symbol: {}, Timestamp: {}, Open: {}, High: {}, Low: {}, Close: {}",
            ohlc.symbol, ohlc.timestamp, ohlc.open, ohlc.high, ohlc.low, ohlc.close
        );
    }

    //write OHLC date into the file
    let ohlc_data = rolling_ohlc.get_ohlc_data();
    let ohlc_json = serde_json::to_string(ohlc_data).unwrap();
    let mut file = File::create("Output.txt").unwrap();
    file.write_all(ohlc_json.as_bytes()).unwrap();
}
