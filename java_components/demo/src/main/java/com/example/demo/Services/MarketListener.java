package com.example.demo.Services;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class MarketListener {

    // This tells Spring: "Watch the 'market.ticks' queue.
    // When a message arrives, run this method."
    @RabbitListener(queues = "market.ticks")
    public void receiveTick(String message) {
        // In the next step, we will parse this JSON into a Java Object
        System.out.println("☕ JAVA RECEIVED TICK: " + message);
    }

    @RabbitListener(queues = "news.headlines")
    public void receiveNews(String message) {
        System.out.println("📰 JAVA RECEIVED NEWS: " + message);
    }
}
