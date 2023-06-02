use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Deserialize, Clone)]
pub struct BookTicker {
    e: String,
    u: u64,
    s: String,
    b: String,
    B: String,
    a: String,
    A: String,
    T: u64,
    E: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OHLC {
    pub symbol: String,
    pub timestamp: u64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
}

#[derive(Clone)]
pub struct RollingOHLC {
    window_size: u64,
    price_data: VecDeque<BookTicker>,
    ohlc_data: Vec<OHLC>,
}

impl RollingOHLC {
    /// Create a new instance of RollingOHLC with the specified window size.
    pub fn new(window_size: u64) -> Self {
        RollingOHLC {
            window_size,
            price_data: VecDeque::new(),
            ohlc_data: vec![],
        }
    }

    /// Update the RollingOHLC with the provided price data and compute the OHLC values.
    /// Returns the computed OHLC values if there is enough price data in the window, otherwise returns None.
    pub fn update(&mut self, pricedata: BookTicker) -> Option<OHLC> {
        // Remove any old price data that is outside the current window
        let oldest_allowed_timestamp = pricedata.T - self.window_size;
        while self
            .price_data
            .front()
            .map_or(false, |pd| pd.T < oldest_allowed_timestamp)
        {
            self.price_data.pop_front();
        }

        // Add the new price data to the rolling window
        self.price_data.push_back(pricedata.clone());

        // If there is enough price data in the window, compute the OHLC data
        if self.price_data.len() >= 4 {
            // Clone the price data and convert it into a vector
            let sorted_prices = self.price_data.clone().into_iter().collect::<Vec<_>>();

            // Determine the number of chunks based on the number of threads available
            let num_chunks = sorted_prices.len().min(rayon::current_num_threads());

            // Calculate the chunk size for parallel processing
            let chunk_size = (sorted_prices.len() + num_chunks - 1) / num_chunks;

            // Perform parallel processing on the chunks of price data
            let ohlc_data = sorted_prices
                .par_chunks(chunk_size)
                .map(|chunk| {
                    // Compute the OHLC values for each chunk
                    let open_price = chunk[0].a.parse::<f64>().unwrap();
                    let close_price = chunk.last().unwrap().a.parse::<f64>().unwrap();
                    let high_price = chunk
                        .iter()
                        .map(|pd| pd.a.parse::<f64>().unwrap())
                        .max_by(|a, b| a.partial_cmp(b).unwrap())
                        .unwrap();
                    let low_price = chunk
                        .iter()
                        .map(|pd| pd.a.parse::<f64>().unwrap())
                        .min_by(|a, b| a.partial_cmp(b).unwrap())
                        .unwrap();

                    OHLC {
                        symbol: pricedata.s.clone(),
                        timestamp: chunk.last().unwrap().T,
                        open: format!("{:.6}", open_price),
                        high: format!("{:.6}", high_price),
                        low: format!("{:.6}", low_price),
                        close: format!("{:.6}", close_price),
                    }
                })
                .collect::<Vec<_>>();

            // Extend the OHLC data with the computed values
            self.ohlc_data.extend(ohlc_data.iter().cloned());

            // Return the last computed OHLC value
            return Some(ohlc_data.last().unwrap().clone());
        }

        None
    }

    /// Get the computed OHLC data.
    pub fn get_ohlc_data(&self) -> &[OHLC] {
        &self.ohlc_data
    }
}
