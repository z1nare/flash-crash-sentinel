package com.example.demo.Services;


import com.example.demo.DTOS.TickerDTO;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedList;


@Service
public class VpinCalculationService {
    private static final double BUCKET_VOLUME = 100000.0;
    private static final int BUCKET_WINDOW = 50;


    // Helper class to hold running state for VPIN calculation per ticker
    private static class VpinState {
        double currentBucketVolume = 0;
        double currentBucketImbalance = 0;
        double lastVpin = 0.0;

        // LinkedList is efficient for maintaining a rolling history
        final LinkedList<Double> imbalanceHistory = new LinkedList<>();
        final LinkedList<Double> volumeHistory = new LinkedList<>();
    }

    HashMap<String, VpinState> tickerFreqHist = new HashMap<>();

    public void processTick(TickerDTO ticker){
        String tick = ticker.getTicker();

        VpinState state = tickerFreqHist.computeIfAbsent(tick, k -> new VpinState());

        double volume = ticker.getVolume();
        double buyVolume = 0.0;
        double sellVolume = 0.0;

        // If price went up, assume all volume was buy-initiated
        if (ticker.getClose() > ticker.getOpen()) {
            buyVolume = volume;
        }
        // If price went down, assume all volume was sell-initiated
        else if (ticker.getClose() < ticker.getOpen()) {
            sellVolume = volume;
        } else {
            // Price is unchanged: split the volume evenly
            buyVolume = volume / 2.0;
            sellVolume = volume / 2.0;
        }

        state.currentBucketVolume += volume;
        state.currentBucketImbalance += (buyVolume - sellVolume);

        if (state.currentBucketVolume > BUCKET_VOLUME) {
            calculateVpin(tick, state);
            // Reset state
            state.currentBucketVolume = 0;
            state.currentBucketImbalance = 0;
        }
    }

    private void calculateVpin(String ticker, VpinState state) {
        double currentOI = Math.abs(state.currentBucketImbalance); // Current Order Imbalance
        // Use the volume that completed the bucket (which might be slightly over the target)
        double currentVolume = state.currentBucketVolume;

        state.imbalanceHistory.add(currentOI);
        state.volumeHistory.add(currentVolume);

        if (state.imbalanceHistory.size() > BUCKET_WINDOW) {
            state.imbalanceHistory.removeFirst();
            state.volumeHistory.removeFirst();
        }

        // Calculate VPIN
        // VPIN = (sum of absolute imbalances) / (sum of total volumes)
        double totalImbalance = state.imbalanceHistory.stream().mapToDouble(Double::doubleValue).sum();
        double totalVolume = state.volumeHistory.stream().mapToDouble(Double::doubleValue).sum();

        if (totalVolume > 0){
            double vpin = totalImbalance / totalVolume;
            state.lastVpin = vpin;
            // --- Log or Publish (Next step is publishing to a new queue) ---
            System.out.printf("📊 VPIN for %s: %.4f (Buckets: %d)\n",
                    ticker, state.lastVpin, state.imbalanceHistory.size());
        }

    }
}
